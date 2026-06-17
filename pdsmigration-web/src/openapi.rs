use crate::errors::ApiError;
use crate::errors::ApiErrorBody;
use utoipa::OpenApi;

use crate::api::*;

#[derive(OpenApi)]
#[openapi(
    paths(
        health_check,
        activate_account_api,
        create_account_api,
        deactivate_account_api,
        export_pds_api,
        import_pds_api,
        request_token_api,
        migrate_preferences_api,
        migrate_plc_api,
        get_service_auth_api,
        enqueue_export_blobs_job_api,
        enqueue_export_repo_job_api,
        enqueue_upload_blobs_job_api,
        get_job_api,
    ),
    components(
        schemas(
            ActivateAccountApiRequest,
            CreateAccountApiRequest,
            DeactivateAccountApiRequest,
            ExportPDSApiRequest,
            ImportPDSApiRequest,
            RequestTokenApiRequest,
            UploadBlobsApiRequest,
            MigratePreferencesApiRequest,
            MigratePlcApiRequest,
            ServiceAuthApiRequest,
            // Jobs
            crate::background_jobs::JobKind,
            crate::background_jobs::JobStatus,
            crate::background_jobs::JobProgress,
            crate::background_jobs::JobRecord,
            crate::api::EnqueueJobResponse,
            ApiError,
            ApiErrorBody
        ),
    ),
    tags(
        (name = "pdsmigration-web", description = "PDS Migration Web API")
    )
)]
pub struct ApiDoc;
