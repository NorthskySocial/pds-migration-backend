use crate::errors::GuiError;
use crate::screens::Screen;
use crate::session::session_config::PdsSession;
use crate::{activate_account, deactivate_account, export_repo, styles, ScreenType};
use bsky_sdk::BskyAgent;
use egui::{ScrollArea, Ui};
use pdsmigration_common::{
    build_agent, login_helper, missing_blobs, upload_blobs_api, UploadBlobsRequest,
};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AdvancedHome {
    pds_session: Arc<RwLock<PdsSession>>,
    error: Arc<RwLock<Vec<GuiError>>>,
    _page: Arc<RwLock<ScreenType>>,
}

impl AdvancedHome {
    pub fn new(
        pds_session: Arc<RwLock<PdsSession>>,
        error: Arc<RwLock<Vec<GuiError>>>,
        _page: Arc<RwLock<ScreenType>>,
    ) -> Self {
        Self {
            pds_session,
            error,
            _page,
        }
    }
}

impl Screen for AdvancedHome {
    fn ui(&mut self, ui: &mut Ui, ctx: &egui::Context) {
        ScrollArea::both().show(ui, |ui| {
            styles::render_button(ui, ctx, "Export Repo", || {
                let pds_session = {
                    let pds_session_lock = self.pds_session.clone();
                    let value = pds_session_lock.blocking_read();
                    value.clone()
                };
                let error = self.error.clone();
                tokio::spawn(async move {
                    match export_repo(pds_session).await {
                        Ok(_) => {}
                        Err(e) => {
                            let mut error = error.write().await;
                            error.push(e);
                        }
                    }
                });
            });
            styles::render_button(ui, ctx, "Activate Account", || {
                let pds_session = {
                    let pds_session_lock = self.pds_session.clone();
                    let value = pds_session_lock.blocking_read();
                    value.clone()
                };
                let error = self.error.clone();
                let new_session_config = match pds_session.old_session_config() {
                    None => {
                        let mut error_write = error.blocking_write();
                        error_write.push(GuiError::Other);
                        return;
                    }
                    Some(config) => config.clone(),
                };
                tokio::spawn(async move {
                    tracing::info!("Activating Account");
                    match activate_account(new_session_config).await {
                        Ok(_) => {
                            tracing::info!("Activated new account");
                        }
                        Err(e) => {
                            let mut error_write = error.write().await;
                            error_write.push(e);
                        }
                    }
                });
            });
            styles::render_button(ui, ctx, "Deactivate Account", || {
                let pds_session = {
                    let pds_session_lock = self.pds_session.clone();
                    let value = pds_session_lock.blocking_read();
                    value.clone()
                };
                let error = self.error.clone();
                let new_session_config = match pds_session.old_session_config() {
                    None => {
                        let mut error_write = error.blocking_write();
                        error_write.push(GuiError::Other);
                        return;
                    }
                    Some(config) => config.clone(),
                };
                tokio::spawn(async move {
                    tracing::info!("Deactivating account");
                    match deactivate_account(new_session_config).await {
                        Ok(_) => {
                            tracing::info!("Deactivated account");
                        }
                        Err(e) => {
                            let mut error_write = error.write().await;
                            error_write.push(e);
                        }
                    }
                });
            });
            styles::render_button(ui, ctx, "Determine Missing Blobs For New Account", || {
                let pds_session = {
                    let pds_session_lock = self.pds_session.clone();
                    let value = pds_session_lock.blocking_read();
                    value.clone()
                };
                let error = self.error.clone();
                let new_session_config = match pds_session.old_session_config() {
                    None => {
                        let mut error_write = error.blocking_write();
                        error_write.push(GuiError::Other);
                        return;
                    }
                    Some(config) => config.clone(),
                };
                tokio::spawn(async move {
                    let agent = build_agent().await.unwrap();
                    let _session = login_helper(
                        &agent,
                        new_session_config.host(),
                        new_session_config.did(),
                        new_session_config.access_token(),
                    )
                    .await
                    .unwrap();
                    tracing::info!("Getting missing blobs");
                    match missing_blobs(&agent).await {
                        Ok(blobs) => {
                            let mut file = File::create("missing_blobs.txt").await.unwrap();
                            for blob in blobs {
                                // Get the CID as a string using Debug formatting
                                let cid_str = format!("{:?}\n", blob.cid);
                                file.write_all(cid_str.as_bytes()).await.unwrap();
                            }
                            tracing::info!("Found missing blobs");
                        }
                        Err(e) => {
                            let mut error_write = error.write().await;
                            error_write.push(GuiError::Other);
                            tracing::error!("{}", e.to_string());
                        }
                    }
                });
            });
            styles::render_button(ui, ctx, "Upload Blobs", || {
                let pds_session = {
                    let pds_session_lock = self.pds_session.clone();
                    let value = pds_session_lock.blocking_read();
                    value.clone()
                };
                let error = self.error.clone();
                let new_session_config = match pds_session.old_session_config() {
                    None => {
                        let mut error_write = error.blocking_write();
                        error_write.push(GuiError::Other);
                        return;
                    }
                    Some(config) => config.clone(),
                };
                tokio::spawn(async move {
                    let agent = BskyAgent::builder().build().await.unwrap();
                    let session = login_helper(
                        &agent,
                        new_session_config.host(),
                        new_session_config.did(),
                        new_session_config.access_token(),
                    )
                    .await
                    .unwrap();
                    tracing::info!("Uploading blobs");
                    let upload_blob_request = UploadBlobsRequest {
                        pds_host: new_session_config.host().to_string(),
                        did: new_session_config.did().to_string(),
                        token: session.access_jwt.clone(),
                        is_missing_blob_request: false,
                    };
                    match upload_blobs_api(upload_blob_request).await {
                        Ok(_) => {
                            tracing::info!("Uploaded blobs");
                        }
                        Err(e) => {
                            let mut error_write = error.write().await;
                            error_write.push(GuiError::Other);
                            tracing::error!("{}", e.to_string());
                        }
                    }
                });
            });
        });
    }

    fn name(&self) -> ScreenType {
        ScreenType::Advanced
    }
}
