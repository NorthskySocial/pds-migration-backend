use crate::agent::{confirm_email_token, login_helper2};
use crate::errors::GuiError;
use crate::screens::Screen;
use crate::session::session_config::PdsSession;
use crate::styles::WIDGET_SPACING_BASE;
use crate::{styles, ScreenType, normalize_pds_host};
use bsky_sdk::BskyAgent;
use egui::Ui;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct OldLogin {
    old_pds_host: String,
    username: String,
    password: String,
    email_token_page: Arc<RwLock<bool>>,
    _email: String,
    email_token: String,
    pds_session: Arc<RwLock<PdsSession>>,
    error: Arc<RwLock<Vec<GuiError>>>,
    page: Arc<RwLock<ScreenType>>,
}

impl OldLogin {
    pub fn new(
        pds_session: Arc<RwLock<PdsSession>>,
        error: Arc<RwLock<Vec<GuiError>>>,
        page: Arc<RwLock<ScreenType>>,
    ) -> Self {
        Self {
            old_pds_host: "".to_string(),
            username: "".to_string(),
            password: "".to_string(),
            email_token_page: Arc::new(Default::default()),
            _email: "".to_string(),
            email_token: "".to_string(),
            pds_session,
            error,
            page,
        }
    }

    #[tracing::instrument(skip(self))]
    fn confirm_email_token(&mut self) {
        let old_pds_host = self.old_pds_host.to_string();
        let username = self.username.to_string();
        let password = self.password.to_string();
        let pds_session = self.pds_session.clone();
        let page_lock = self.page.clone();
        let error_lock = self.error.clone();
        let email_token_page_lock = self.email_token_page.clone();
        let email_token = self.email_token.clone();

        tokio::spawn(async move {
            tracing::info!("Confirming email token");
            let bsky_agent = BskyAgent::builder().build().await.unwrap();

            match confirm_email_token(
                &bsky_agent,
                old_pds_host.as_str(),
                username.as_str(),
                password.as_str(),
                email_token.as_str(),
            )
            .await
            {
                Ok(res) => {
                    tracing::info!("Successfully logged in to old PDS");
                    let old_pds_token = res.access_jwt.clone();
                    let old_pds_refresh = res.refresh_jwt.clone();
                    let did = res.did.as_str().to_string();
                    let mut pds_session = pds_session.write().await;
                    if pds_session
                        .create_old_session(
                            did.as_str(),
                            old_pds_token.as_str(),
                            old_pds_refresh.as_str(),
                            old_pds_host.as_str(),
                        )
                        .is_err()
                    {
                        tracing::error!("Unexpected error occurred");
                        let mut error = error_lock.write().await;
                        error.push(GuiError::Runtime);
                    }
                    let mut page = page_lock.write().await;
                    *page = ScreenType::Basic;
                    drop(page)
                }
                Err(e) => {
                    if let GuiError::AuthFactorTokenRequired = e {
                        let mut email_token_page = email_token_page_lock.write().await;
                        *email_token_page = true;
                    } else {
                        tracing::error!("Error logging in to old PDS: {:?}", e);
                        let mut error = error_lock.write().await;
                        error.push(e);
                    }
                }
            };
        });
    }

    #[tracing::instrument(skip(self))]
    fn old_session_login(&mut self) {
        let old_pds_host = self.old_pds_host.to_string();
        let username = self.username.to_string();
        let password = self.password.to_string();
        let pds_session = self.pds_session.clone();
        let page_lock = self.page.clone();
        let error_lock = self.error.clone();
        let email_token_page_lock = self.email_token_page.clone();

        tokio::spawn(async move {
            tracing::info!("Logging in to old PDS");
            let bsky_agent = BskyAgent::builder().build().await.unwrap();

            match login_helper2(
                &bsky_agent,
                old_pds_host.as_str(),
                username.as_str(),
                password.as_str(),
            )
            .await
            {
                Ok(res) => {
                    tracing::info!("Successfully logged in to old PDS");
                    let old_pds_token = res.access_jwt.clone();
                    let old_pds_refresh = res.refresh_jwt.clone();
                    let did = res.did.as_str().to_string();
                    let mut pds_session = pds_session.write().await;
                    if pds_session
                        .create_old_session(
                            did.as_str(),
                            old_pds_token.as_str(),
                            old_pds_refresh.as_str(),
                            old_pds_host.as_str(),
                        )
                        .is_err()
                    {
                        tracing::error!("Unexpected error occurred");
                        let mut error = error_lock.write().await;
                        error.push(GuiError::Runtime);
                    }
                    let mut page = page_lock.write().await;
                    *page = ScreenType::Basic;
                    drop(page)
                }
                Err(e) => {
                    if let GuiError::AuthFactorTokenRequired = e {
                        let mut email_token_page = email_token_page_lock.write().await;
                        *email_token_page = true;
                    } else {
                        tracing::error!("Error logging in to old PDS: {:?}", e);
                        let mut error = error_lock.write().await;
                        error.push(e);
                    }
                }
            };
        });
    }

    fn validate_inputs(&mut self) -> bool {
        if normalize_pds_host(&mut self.old_pds_host).is_err() {
            return false;
        }
        let username = self.username.to_string();
        let password = self.password.to_string();

        if username.is_empty() {
            tracing::error!("Username cannot be empty");
            return false;
        }

        if password.is_empty() {
            tracing::error!("Password cannot be empty");
            return false;
        }

        true
    }
}

impl Screen for OldLogin {
    fn ui(&mut self, ui: &mut Ui, ctx: &egui::Context) {
        let email_token_page = {
            let email_token_page = self.email_token_page.blocking_read();
            *email_token_page
        };
        if !email_token_page {
            styles::render_subtitle(ui, ctx, "Current PDS Login");
            ui.vertical_centered(|ui| {
                styles::render_input(
                    ui,
                    "Current PDS Host",
                    &mut self.old_pds_host,
                    false,
                    Some("https://bsky.social"),
                );
                styles::render_input(
                    ui,
                    "Handle",
                    &mut self.username,
                    false,
                    Some("myaccount.bsky.social"),
                );
                styles::render_input(ui, "Password", &mut self.password, true, None);
                ui.add_space(WIDGET_SPACING_BASE);
                styles::render_button(ui, ctx, "Submit", || {
                    if self.validate_inputs() {
                        self.old_session_login();
                    }
                });
            });
        } else {
            styles::render_subtitle(ui, ctx, "Current PDS Login");
            ui.vertical_centered(|ui| {
                styles::render_input(
                    ui,
                    "Validate Email Token",
                    &mut self.email_token,
                    false,
                    None,
                );
                ui.add_space(WIDGET_SPACING_BASE);
                styles::render_button(ui, ctx, "Submit", || {
                    self.confirm_email_token();
                });
            });
        }
    }

    fn name(&self) -> ScreenType {
        ScreenType::OldLogin
    }
}
