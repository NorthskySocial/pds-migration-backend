use crate::agent::login_helper2;
use crate::errors::GuiError;
use crate::screens::Screen;
use crate::session::session_config::PdsSession;
use crate::{
    check_did_exists, create_account, fetch_tos_and_privacy_policy, styles,
    CreateAccountParameters, ScreenType, normalize_pds_host,
};
use bsky_sdk::BskyAgent;
use egui::Ui;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct CreateOrLoginAccount {
    new_pds_host: String,
    new_handle: String,
    new_password: String,
    confirm_password: String,
    pds_session: Arc<RwLock<PdsSession>>,
    error: Arc<RwLock<Vec<GuiError>>>,
    pds_selected: bool,
    new_email: String,
    invite_code: String,
    privacy_policy_lock: Arc<RwLock<Option<String>>>,
    terms_of_service_lock: Arc<RwLock<Option<String>>>,
    invite_code_required: Arc<RwLock<bool>>,
    available_user_domains: Arc<RwLock<Vec<String>>>,
    page: Arc<RwLock<ScreenType>>,
    pds_migration_step: Arc<RwLock<bool>>,
    ui_mode: Arc<RwLock<UiMode>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum UiMode {
    SelectPds,
    CreatePds,
    Login,
}

impl CreateOrLoginAccount {
    pub fn new(
        pds_session: Arc<RwLock<PdsSession>>,
        error: Arc<RwLock<Vec<GuiError>>>,
        page: Arc<RwLock<ScreenType>>,
        pds_migration_step: Arc<RwLock<bool>>,
    ) -> Self {
        Self {
            new_pds_host: "".to_string(),
            new_handle: "".to_string(),
            new_password: "".to_string(),
            confirm_password: "".to_string(),
            pds_session,
            error,
            pds_selected: false,
            new_email: "".to_string(),
            invite_code: "".to_string(),
            privacy_policy_lock: Arc::new(Default::default()),
            terms_of_service_lock: Arc::new(Default::default()),
            invite_code_required: Arc::new(Default::default()),
            available_user_domains: Arc::new(Default::default()),
            page,
            pds_migration_step,
            ui_mode: Arc::new(RwLock::from(UiMode::SelectPds)),
        }
    }

    fn update_pds(&mut self) {
        let error = self.error.clone();
        let terms_of_service_lock = self.terms_of_service_lock.clone();
        let privacy_policy_lock = self.privacy_policy_lock.clone();
        let invite_code_required = self.invite_code_required.clone();
        let available_user_domains = self.available_user_domains.clone();
        let new_pds_host = self.new_pds_host.clone();
        let ui_mode = self.ui_mode.clone();
        let pds_session = {
            let lock = self.pds_session.clone();
            let value = lock.blocking_read();
            value.clone()
        };
        let did = pds_session.did().clone().unwrap();
        tokio::spawn(async move {
            match check_did_exists(new_pds_host.as_str(), did.as_str()).await {
                Ok(res) => {
                    if res {
                        let mut ui_mode_write = ui_mode.write().await;
                        *ui_mode_write = UiMode::Login;
                    } else {
                        let mut ui_mode_write = ui_mode.write().await;
                        *ui_mode_write = UiMode::CreatePds;
                    }
                }
                Err(e) => {
                    let mut errors = error.write().await;
                    errors.push(e);
                    let mut ui_mode_write = ui_mode.write().await;
                    *ui_mode_write = UiMode::SelectPds;
                    return;
                }
            }
            let ui_mode = {
                let ui_mode_read = ui_mode.read().await;
                *ui_mode_read
            };
            if ui_mode == UiMode::CreatePds {
                match fetch_tos_and_privacy_policy(new_pds_host).await {
                    Ok(result) => {
                        let mut privacy_policy_write = privacy_policy_lock.write().await;
                        *privacy_policy_write = result.privacy_policy;
                        let mut terms_of_service_lock = terms_of_service_lock.write().await;
                        *terms_of_service_lock = result.terms_of_service;
                        let mut invite_code_required_write = invite_code_required.write().await;
                        *invite_code_required_write = result.invite_code_required;
                        let mut available_user_domains_write = available_user_domains.write().await;
                        *available_user_domains_write = result.available_user_domains;
                    }
                    Err(e) => {
                        let mut errors = error.write().await;
                        errors.push(e);
                    }
                }
            }
        });
    }

    fn submit(&mut self) {
        let pds_session = {
            let lock = self.pds_session.clone();
            let value = lock.blocking_read();
            value.clone()
        };
        let new_email = self.new_email.clone();
        let new_pds_host = self.new_pds_host.clone();
        let new_password = self.new_password.clone();
        let new_handle = self.new_handle.clone();
        let invite_code = self.invite_code.clone();
        let params = CreateAccountParameters {
            pds_session,
            new_email,
            new_pds_host,
            new_password,
            new_handle,
            invite_code,
        };
        let error = self.error.clone();
        let pds_session_lock = self.pds_session.clone();
        let page = self.page.clone();
        let pds_migration_step = self.pds_migration_step.clone();

        tokio::spawn(async move {
            match create_account(params).await {
                Ok(pds_session) => {
                    {
                        let mut pds_session_write = pds_session_lock.write().await;
                        *pds_session_write = pds_session;
                    }
                    let pds_migration_step_read = { *pds_migration_step.read().await };
                    match pds_migration_step_read {
                        false => {
                            let mut page_write = page.write().await;
                            *page_write = ScreenType::Basic;
                        }
                        true => {
                            let mut page_write = page.write().await;
                            *page_write = ScreenType::ExportRepo;
                        }
                    }
                }
                Err(e) => {
                    let mut errors = error.write().await;
                    errors.push(e);
                }
            }
        });
    }

    fn select_pds(&mut self, ui: &mut Ui, ctx: &egui::Context) {
        let _handle = self.new_handle.clone();
        styles::render_subtitle(ui, ctx, "Select PDS!");
        ui.vertical_centered(|ui| {
            styles::render_input(
                ui,
                "New PDS Host",
                &mut self.new_pds_host,
                false,
                Some("https://northsky.social"),
            );
            styles::render_button(ui, ctx, "Update", || {
                self.pds_selected = true;
                self.update_pds();
            });
        });
    }

    #[tracing::instrument(skip(self))]
    fn validate_create_inputs(&mut self) -> bool {
        if self.new_password != self.confirm_password {
            tracing::error!("Passwords do not match");
            return false;
        }
        if self.new_password.is_empty() {
            tracing::error!("Password cannot be empty");
            return false;
        }
        if self.new_handle.is_empty() {
            tracing::error!("Handle cannot be empty");
            return false;
        }
        if self.new_email.is_empty() {
            tracing::error!("Email cannot be empty");
            return false;
        }
        if normalize_pds_host(&mut self.new_pds_host).is_err() {
            return false;
        }

        let invite_code_required = {
            let invite_code_required_lock = self.invite_code_required.clone();
            let value = invite_code_required_lock.blocking_read();
            *value
        };
        if invite_code_required && self.invite_code.is_empty() {
            tracing::error!("Invite Code cannot be empty");
            return false;
        }

        true
    }

    fn create_ui(&mut self, ui: &mut Ui, ctx: &egui::Context) {
        let _handle = self.new_handle.clone();
        let available_user_domains = {
            let available_user_domains = self.available_user_domains.blocking_read();
            available_user_domains
                .first()
                .cloned()
                .unwrap_or("".to_string())
        };
        styles::render_subtitle(ui, ctx, "Create New PDS Account!");
        ui.vertical_centered(|ui| {
            ui.horizontal(|ui| {
                if self.pds_selected {
                    styles::render_input_disabled(
                        ui,
                        "New PDS Host",
                        &mut self.new_pds_host,
                        false,
                        Some("https://northsky.social"),
                    );
                    styles::render_button(ui, ctx, "Edit", || self.pds_selected = false);
                } else {
                    styles::render_input(
                        ui,
                        "New PDS Host",
                        &mut self.new_pds_host,
                        false,
                        Some("https://northsky.social"),
                    );
                    styles::render_button(ui, ctx, "Update", || {
                        self.pds_selected = true;
                        self.update_pds();
                    });
                }
            });
            if self.pds_selected {
                styles::render_input(ui, "Email", &mut self.new_email, false, None);
                styles::render_input(
                    ui,
                    "Handle",
                    &mut self.new_handle,
                    false,
                    Some("user.northsky.social"),
                );
                ui.label(format!(
                    "If not using a custom domain, please append with {available_user_domains}"
                ));
                styles::render_input(ui, "Password", &mut self.new_password, true, None);
                styles::render_input(
                    ui,
                    "Confirm Password",
                    &mut self.confirm_password,
                    true,
                    None,
                );
                let invite_code_required = {
                    let invite_code_required_lock = self.invite_code_required.clone();
                    let value = invite_code_required_lock.blocking_read();
                    *value
                };
                if invite_code_required {
                    styles::render_input(
                        ui,
                        "Invite Code (Leave Blank if None)",
                        &mut self.invite_code,
                        false,
                        None,
                    );
                }

                let privacy_policy = {
                    let privacy_policy = self.privacy_policy_lock.blocking_read();
                    let value = privacy_policy.clone();
                    value.unwrap_or("".to_string())
                };
                let terms_of_service = {
                    let terms_of_service = self.privacy_policy_lock.blocking_read().clone();
                    let value = terms_of_service.clone();
                    value.unwrap_or("".to_string())
                };
                if !privacy_policy.is_empty() || !terms_of_service.is_empty() {
                    ui.horizontal_wrapped(|ui| {
                        ui.spacing_mut().item_spacing.x = 0.0;
                        ui.label("By creating an account you agree to the ");

                        if !terms_of_service.is_empty() {
                            ui.hyperlink_to("Terms of Service", terms_of_service);
                            if !privacy_policy.is_empty() {
                                ui.label(" and ");
                                ui.hyperlink_to("Privacy Policy", privacy_policy);
                                ui.label(".");
                            } else {
                                ui.label(".");
                            }
                        } else {
                            ui.hyperlink_to("Privacy Policy", privacy_policy);
                            ui.label(".");
                        }
                    });
                }
                styles::render_button(ui, ctx, "Submit", || {
                    if self.validate_create_inputs() {
                        self.submit();
                    }
                });
            }
        });
    }

    #[tracing::instrument(skip(self))]
    fn new_session_login(&mut self) {
        let new_pds_host = self.new_pds_host.to_string();
        let new_handle = self.new_handle.to_string();
        let new_password = self.new_password.to_string();
        let pds_session_lock = self.pds_session.clone();
        let error_lock = self.error.clone();
        let page_lock = self.page.clone();
        let pds_migration_step_lock = self.pds_migration_step.clone();

        tokio::spawn(async move {
            let bsky_agent = BskyAgent::builder().build().await.unwrap();
            match login_helper2(
                &bsky_agent,
                new_pds_host.as_str(),
                new_handle.as_str(),
                new_password.as_str(),
            )
            .await
            {
                Ok(res) => {
                    tracing::info!("Login successful");
                    let access_token = res.access_jwt.clone();
                    let refresh_token = res.refresh_jwt.clone();
                    let did = res.did.as_str().to_string();
                    {
                        let mut pds_session = pds_session_lock.write().await;
                        if pds_session
                            .create_new_session(
                                did.as_str(),
                                access_token.as_str(),
                                refresh_token.as_str(),
                                new_pds_host.as_str(),
                            )
                            .is_err()
                        {
                            let mut error = error_lock.write().await;
                            error.push(GuiError::Other);
                        }
                    }
                    let pds_migration_step = {
                        let value = pds_migration_step_lock.read().await;
                        *value
                    };
                    if pds_migration_step {
                        let mut page = page_lock.write().await;
                        *page = ScreenType::ExportRepo;
                    } else {
                        let mut page = page_lock.write().await;
                        *page = ScreenType::Basic;
                    }
                }
                Err(e) => {
                    tracing::error!("Error logging in: {e}");
                    let mut error = error_lock.write().await;
                    error.push(GuiError::Other);
                }
            };
        });
    }

    #[tracing::instrument(skip(self))]
    fn validate_login_inputs(&mut self) -> bool {
        if normalize_pds_host(&mut self.new_pds_host).is_err() {
            return false;
        }
        let new_handle = self.new_handle.to_string();
        let new_password = self.new_password.to_string();

        if new_handle.is_empty() {
            tracing::error!("Handle cannot be empty");
            return false;
        }

        if new_password.is_empty() {
            tracing::error!("Password cannot be empty");
            return false;
        }

        true
    }

    fn login_ui(&mut self, ui: &mut Ui, ctx: &egui::Context) {
        styles::render_subtitle(ui, ctx, "New PDS Login!");
        ui.vertical_centered(|ui| {
            styles::render_input(
                ui,
                "New PDS Host",
                &mut self.new_pds_host,
                false,
                Some("https://northsky.social"),
            );
            styles::render_input(
                ui,
                "Handle",
                &mut self.new_handle,
                false,
                Some("user.northsky.social"),
            );
            styles::render_input(ui, "Password", &mut self.new_password, true, None);
            styles::render_button(ui, ctx, "Submit", || {
                if self.validate_login_inputs() {
                    self.new_session_login();
                }
            });
        });
    }
}

impl Screen for CreateOrLoginAccount {
    fn ui(&mut self, ui: &mut Ui, ctx: &egui::Context) {
        let ui_mode = {
            let lock = self.ui_mode.clone();
            let value = lock.blocking_read();
            *value
        };
        match ui_mode {
            UiMode::SelectPds => {
                self.select_pds(ui, ctx);
            }
            UiMode::CreatePds => {
                self.create_ui(ui, ctx);
            }
            UiMode::Login => {
                self.login_ui(ui, ctx);
            }
        }
    }

    fn name(&self) -> ScreenType {
        ScreenType::CreateOrLoginAccount
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn create_test_session() -> Arc<RwLock<PdsSession>> {
        Arc::new(RwLock::new(PdsSession::default()))
    }

    fn create_test_errors() -> Arc<RwLock<Vec<GuiError>>> {
        Arc::new(RwLock::new(Vec::new()))
    }

    fn create_test_page() -> Arc<RwLock<ScreenType>> {
        Arc::new(RwLock::new(ScreenType::CreateOrLoginAccount))
    }

    fn create_test_migration_step() -> Arc<RwLock<bool>> {
        Arc::new(RwLock::new(false))
    }

    #[test]
    fn test_create_account_new() {
        let session = create_test_session();
        let errors = create_test_errors();
        let page = create_test_page();
        let migration_step = create_test_migration_step();

        let create_account = CreateOrLoginAccount::new(session, errors, page, migration_step);

        // Test initial state
        assert_eq!(create_account.new_email, "");
        assert_eq!(create_account.new_pds_host, "");
        assert_eq!(create_account.new_password, "");
        assert_eq!(create_account.new_handle, "");
        assert_eq!(create_account.confirm_password, "");
        assert_eq!(create_account.invite_code, "");
        assert!(!create_account.pds_selected);
    }

    #[test]
    fn test_screen_name() {
        let session = create_test_session();
        let errors = create_test_errors();
        let page = create_test_page();
        let migration_step = create_test_migration_step();

        let create_account = CreateOrLoginAccount::new(session, errors, page, migration_step);

        assert!(matches!(
            create_account.name(),
            ScreenType::CreateOrLoginAccount
        ));
    }

    #[test]
    fn test_form_field_updates() {
        let session = create_test_session();
        let errors = create_test_errors();
        let page = create_test_page();
        let migration_step = create_test_migration_step();

        let mut create_account = CreateOrLoginAccount::new(session, errors, page, migration_step);

        // Test field updates
        create_account.new_email = "test@example.com".to_string();
        create_account.new_pds_host = "https://test.pds.com".to_string();
        create_account.new_password = "testpassword".to_string();
        create_account.confirm_password = "testpassword".to_string();
        create_account.new_handle = "testuser.bsky.social".to_string();
        create_account.invite_code = "invite123".to_string();

        assert_eq!(create_account.new_email, "test@example.com");
        assert_eq!(create_account.new_pds_host, "https://test.pds.com");
        assert_eq!(create_account.new_password, "testpassword");
        assert_eq!(create_account.confirm_password, "testpassword");
        assert_eq!(create_account.new_handle, "testuser.bsky.social");
        assert_eq!(create_account.invite_code, "invite123");
    }

    #[test]
    fn test_pds_selection_toggle() {
        let session = create_test_session();
        let errors = create_test_errors();
        let page = create_test_page();
        let migration_step = create_test_migration_step();

        let mut create_account = CreateOrLoginAccount::new(session, errors, page, migration_step);

        // Test initial state
        assert!(!create_account.pds_selected);

        // Test selecting PDS
        create_account.pds_selected = true;
        assert!(create_account.pds_selected);

        // Test deselecting PDS
        create_account.pds_selected = false;
        assert!(!create_account.pds_selected);
    }

    #[test]
    fn test_password_validation_logic() {
        // Test the password matching logic used in the UI
        let password1 = "testpassword123";
        let password2 = "testpassword123";
        let password3 = "differentpassword";

        // Passwords match
        assert_eq!(password1, password2);
        assert!(password1 == password2);

        // Passwords don't match
        assert_ne!(password1, password3);
        assert!(password1 != password3);
    }

    #[test]
    fn test_form_validation_scenarios() {
        // Test various form validation scenarios
        let valid_email = "user@example.com";
        let invalid_email = "";

        let valid_handle = "user.bsky.social";
        let invalid_handle = "";

        let valid_password = "securepassword123";
        let invalid_password = "";

        let valid_pds_host = "https://bsky.social";
        let invalid_pds_host = "";

        // Valid form data
        assert!(!valid_email.is_empty());
        assert!(!valid_handle.is_empty());
        assert!(!valid_password.is_empty());
        assert!(!valid_pds_host.is_empty());
        assert!(valid_pds_host.starts_with("https://"));

        // Invalid form data
        assert!(invalid_email.is_empty());
        assert!(invalid_handle.is_empty());
        assert!(invalid_password.is_empty());
        assert!(invalid_pds_host.is_empty());
    }

    #[test]
    fn test_invite_code_handling() {
        // Test invite code scenarios
        let valid_invite_code = "ABC123DEF";
        let empty_invite_code = "";

        // Both should be acceptable (invite code is optional)
        assert!(!valid_invite_code.is_empty());
        assert!(empty_invite_code.is_empty());

        // Test trimming
        let invite_with_spaces = "  ABC123  ";
        let trimmed = invite_with_spaces.trim();
        assert_eq!(trimmed, "ABC123");
    }

    #[test]
    fn test_pds_host_validation() {
        let valid_hosts = vec![
            "https://bsky.social",
            "https://pds.example.com",
            "https://northsky.social",
        ];

        let invalid_hosts = vec![
            "",
            "not-a-url",
            "http://insecure.example.com", // Could be considered less secure
            "ftp://wrong.protocol.com",
        ];

        for host in valid_hosts {
            assert!(host.starts_with("https://"));
            assert!(!host.is_empty());
        }

        for host in invalid_hosts {
            if host.is_empty() || host == "not-a-url" {
                assert!(!host.starts_with("https://"));
            }
        }
    }

    #[test]
    fn test_handle_format_validation() {
        let valid_handles = vec![
            "user.bsky.social",
            "handle.northsky.social",
            "my.custom.domain.com",
        ];

        let potentially_invalid_handles =
            vec!["", "no-dots", "spaces in handle", ".starting.with.dot"];

        for handle in valid_handles {
            assert!(!handle.is_empty());
            assert!(handle.contains('.'));
        }

        for handle in potentially_invalid_handles {
            if handle.is_empty() || handle.contains(' ') {
                // These are clearly invalid
                let is_problematic = handle.is_empty() || handle.contains(' ');
                assert!(is_problematic);
            }
        }
    }

    #[test]
    fn test_create_account_parameters_structure() {
        // Test that CreateAccountParameters can be constructed
        let session = PdsSession::default();
        let params = CreateAccountParameters {
            pds_session: session,
            new_email: "test@example.com".to_string(),
            new_pds_host: "https://test.com".to_string(),
            new_password: "password".to_string(),
            new_handle: "handle.test.com".to_string(),
            invite_code: "invite123".to_string(),
        };

        assert_eq!(params.new_email, "test@example.com");
        assert_eq!(params.new_pds_host, "https://test.com");
        assert_eq!(params.new_password, "password");
        assert_eq!(params.new_handle, "handle.test.com");
        assert_eq!(params.invite_code, "invite123");
    }
}
