use crate::errors::GuiError;
use crate::screens::Screen;
use crate::session::session_config::PdsSession;
use crate::{generate_recovery_key, migrate_plc_via_pds, request_token, styles, ScreenType};
use egui::Ui;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct MigratePLC {
    user_recovery_key_password: String,
    pds_session: Arc<RwLock<PdsSession>>,
    error: Arc<RwLock<Vec<GuiError>>>,
    task_started: bool,
    user_recovery_key: String,
    generated_user_recovery_key: Arc<RwLock<Option<String>>>,
    plc_token: String,
    page: Arc<RwLock<ScreenType>>,
}

impl MigratePLC {
    pub fn new(
        pds_session: Arc<RwLock<PdsSession>>,
        error: Arc<RwLock<Vec<GuiError>>>,
        page: Arc<RwLock<ScreenType>>,
        _pds_migration_step: Arc<RwLock<bool>>,
    ) -> Self {
        Self {
            user_recovery_key_password: "".to_string(),
            pds_session,
            error,
            task_started: false,
            user_recovery_key: "".to_string(),
            generated_user_recovery_key: Arc::new(Default::default()),
            plc_token: "".to_string(),
            page,
        }
    }

    fn show(&mut self, ui: &mut Ui, ctx: &egui::Context) {
        let error_lock = self.error.clone();
        styles::render_button(ui, ctx, "Request Token", || {
            let session_config = {
                let lock = self.pds_session.clone();
                let value = lock.blocking_read();
                let pds_session = value.clone();
                match pds_session.old_session_config() {
                    Some(session) => session.clone(),
                    None => {
                        let mut error_write = error_lock.blocking_write();
                        error_write.push(GuiError::Other);
                        return;
                    }
                }
            };
            tokio::spawn(async move {
                match request_token(session_config).await {
                    Ok(_) => {
                        tracing::info!("Token requested");
                    }
                    Err(e) => {
                        let mut error_write = error_lock.blocking_write();
                        error_write.push(e);
                    }
                }
            });
        });
        styles::render_subtitle(ui, ctx, "Create Recovery Key");
        ui.horizontal(|ui| {
            styles::render_button(ui, ctx, "Generate Recovery Key", || {
                if self.user_recovery_key_password.is_empty() {
                    tracing::error!("User Recovery Key Password is empty");
                    return;
                }

                let user_recovery_key_password = self.user_recovery_key_password.clone();
                let generated_user_recovery_key = self.generated_user_recovery_key.clone();
                let error_lock = self.error.clone();
                tokio::spawn(async move {
                    match generate_recovery_key(user_recovery_key_password) {
                        Ok(key) => {
                            let mut generated_user_recovery_key_write =
                                generated_user_recovery_key.write().await;
                            *generated_user_recovery_key_write = Some(key);
                        }
                        Err(e) => {
                            let mut error_write = error_lock.write().await;
                            error_write.push(e);
                        }
                    }
                });
            });
            styles::render_input(
                ui,
                "Archive Password",
                &mut self.user_recovery_key_password,
                true,
                Some(""),
            );
        });

        ui.horizontal(|ui| {
            ui.horizontal(|ui| {
                ui.vertical(|ui| {
                    ui.label("PLC Signing Token");
                    ui.text_edit_singleline(&mut self.plc_token);
                });
                let generated_user_recovery_key_lock = self.generated_user_recovery_key.clone();
                let generated_user_recovery_key = {
                    let lock = generated_user_recovery_key_lock.blocking_read();
                    lock.clone()
                };
                match generated_user_recovery_key {
                    None => {}
                    Some(key) => {
                        self.user_recovery_key = key;
                    }
                }
                ui.vertical(|ui| {
                    ui.label("User Recovery Key");
                    ui.text_edit_singleline(&mut self.user_recovery_key);
                });
            });
        });
        ui.horizontal(|ui| {
            styles::render_button(ui, ctx, "Submit", || {
                self.plc_token = self.plc_token.trim().to_string();
                self.user_recovery_key = self.user_recovery_key.trim().to_string();
                if self.plc_token.is_empty() {
                    tracing::error!("PLC Signing Token is empty");
                    return;
                }
                if self.user_recovery_key.is_empty() {
                    tracing::error!("User Recovery Key is empty");
                    return;
                }
                if self
                    .pds_session
                    .blocking_read()
                    .old_session_config()
                    .is_none()
                {
                    tracing::error!("No active old PDS session; please log in first");
                    return;
                }

                self.task_started = true;
                self.start_migration();
            });
        });
    }

    fn start_migration(&mut self) {
        let pds_session = {
            let lock = self.pds_session.clone();
            let value = lock.blocking_read();
            value.clone()
        };
        let plc_signing_token = self.plc_token.clone();
        let user_recovery_key = self.user_recovery_key.clone();
        let error = self.error.clone();
        let page = self.page.clone();
        tokio::spawn(async move {
            tracing::info!("Updating PLC Directory");
            match migrate_plc_via_pds(pds_session, plc_signing_token, Some(user_recovery_key)).await
            {
                Ok(_) => {
                    tracing::info!("PLC Directory updated");
                    let mut page_write = page.write().await;
                    *page_write = ScreenType::ActiveAccounts;
                }
                Err(e) => {
                    tracing::error!("Error updating PLC Directory: {}", e);
                    let mut error_write = error.write().await;
                    error_write.push(e);
                }
            }
        });
    }
}

impl Screen for MigratePLC {
    fn ui(&mut self, ui: &mut Ui, ctx: &egui::Context) {
        styles::render_subtitle(ui, ctx, "Updating PLC");
        if !self.task_started {
            self.show(ui, ctx);
        }
    }

    fn name(&self) -> ScreenType {
        ScreenType::MigratePLC
    }
}
