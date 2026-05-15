use eframe::egui::{self, Color32, RichText, ScrollArea, Ui};
use egui_file_dialog::FileDialog;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

#[derive(Clone, Debug)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug)]
pub struct LogEntry {
    timestamp: SystemTime,
    level: LogLevel,
    message: String,
}

impl LogEntry {
    pub fn new(level: LogLevel, message: String) -> Self {
        Self {
            timestamp: SystemTime::now(),
            level,
            message,
        }
    }

    fn level_color(&self) -> Color32 {
        match self.level {
            LogLevel::Debug => Color32::from_rgb(150, 150, 150),
            LogLevel::Info => Color32::from_rgb(100, 200, 100),
            LogLevel::Warning => Color32::from_rgb(255, 180, 0),
            LogLevel::Error => Color32::from_rgb(255, 0, 0),
        }
    }

    fn level_prefix(&self) -> &'static str {
        match self.level {
            LogLevel::Debug => "[DEBUG]",
            LogLevel::Info => "[INFO]",
            LogLevel::Warning => "[WARN]",
            LogLevel::Error => "[ERROR]",
        }
    }
}

#[derive(Clone)]
pub struct LogBuffer {
    entries: Arc<Mutex<VecDeque<LogEntry>>>,
    max_entries: usize,
}

impl Default for LogBuffer {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::new())),
            max_entries: 100000,
        }
    }
}

impl LogBuffer {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::new())),
            max_entries,
        }
    }

    pub fn add_entry(&self, entry: LogEntry) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.push_back(entry);
            // Ensure we don't exceed max entries by removing from front
            while entries.len() > self.max_entries {
                entries.pop_front();
            }
        }
    }

    pub fn debug(&self, message: impl Into<String>) {
        self.add_entry(LogEntry::new(LogLevel::Debug, message.into()));
    }

    pub fn info(&self, message: impl Into<String>) {
        self.add_entry(LogEntry::new(LogLevel::Info, message.into()));
    }

    pub fn warning(&self, message: impl Into<String>) {
        self.add_entry(LogEntry::new(LogLevel::Warning, message.into()));
    }

    pub fn error(&self, message: impl Into<String>) {
        self.add_entry(LogEntry::new(LogLevel::Error, message.into()));
    }

    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }

    /// Exports all log entries to a text file at the specified path.
    /// Returns Ok(()) on success, or an error if the file couldn't be created or written.
    pub fn export_to_file(&self, path: &str) -> Result<(), std::io::Error> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(path)?;

        if let Ok(entries) = self.entries.lock() {
            for entry in entries.iter() {
                // Format timestamp
                let time_since_epoch = entry
                    .timestamp
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default();
                let secs = time_since_epoch.as_secs();
                let millis = time_since_epoch.subsec_millis();

                let hours = (secs / 3600) % 24;
                let minutes = (secs / 60) % 60;
                let seconds = secs % 60;

                let timestamp = format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, seconds, millis);

                // Write formatted log entry to file
                writeln!(
                    file,
                    "{} {} {}",
                    timestamp,
                    entry.level_prefix(),
                    entry.message
                )?;
            }
        }

        Ok(())
    }
}

pub struct LogViewer {
    buffer: LogBuffer,
    show_debug: bool,
    show_info: bool,
    show_warning: bool,
    show_error: bool,
    filter_text: String,
    auto_scroll: bool,
    _export_dialog: Option<FileDialog>,
}

impl Default for LogViewer {
    fn default() -> Self {
        Self {
            buffer: LogBuffer::default(),
            show_debug: true,
            show_info: true,
            show_warning: true,
            show_error: true,
            filter_text: String::new(),
            auto_scroll: true,
            _export_dialog: None,
        }
    }
}

impl LogViewer {
    pub fn new(buffer: LogBuffer) -> Self {
        Self {
            buffer,
            show_debug: true,
            show_info: true,
            show_warning: true,
            show_error: true,
            filter_text: String::new(),
            auto_scroll: true,
            _export_dialog: None,
        }
    }

    pub fn ui(&mut self, ui: &mut Ui) {
        ui.vertical(|ui| {
            // Controls section
            ui.horizontal(|ui| {
                ui.checkbox(&mut self.show_debug, "Debug");
                ui.checkbox(&mut self.show_info, "Info");
                ui.checkbox(&mut self.show_warning, "Warning");
                ui.checkbox(&mut self.show_error, "Error");
                ui.separator();
                ui.checkbox(&mut self.auto_scroll, "Auto-scroll");
                ui.separator();

                if ui.button("Clear").clicked() {
                    self.buffer.clear();
                }

                ui.separator();
                ui.label("Filter:");

                ui.text_edit_singleline(&mut self.filter_text);
                ui.separator();
                if ui.button("Export").clicked() {
                    // Open a file dialog to let the user choose where to save the log file
                    let path = rfd::FileDialog::new()
                        .set_title("Save Log File")
                        .set_file_name("log.txt")
                        .set_directory(".")
                        .add_filter("Text Files", &["txt"])
                        .save_file();

                    if let Some(path) = path {
                        if let Err(e) = self
                            .buffer
                            .export_to_file(path.to_str().unwrap_or("log.txt"))
                        {
                            // Log an error if the export fails
                            self.buffer.error(format!("Failed to export log: {}", e));
                        } else {
                            // Log success
                            self.buffer
                                .info(format!("Log exported to {}", path.display()));
                        }
                    }
                }
            });

            // Log entries section
            ui.separator();

            let mut scroll_to_bottom = false;
            let text_height = ui.text_style_height(&egui::TextStyle::Body);
            let available_height = ui.available_height() - text_height;

            ScrollArea::vertical()
                .auto_shrink([false, false])
                .stick_to_bottom(self.auto_scroll)
                .show_viewport(ui, |ui, _viewport| {
                    ui.set_min_height(available_height);

                    if let Ok(entries) = self.buffer.entries.lock() {
                        for entry in entries.iter() {
                            let show_entry = match entry.level {
                                LogLevel::Debug => self.show_debug,
                                LogLevel::Info => self.show_info,
                                LogLevel::Warning => self.show_warning,
                                LogLevel::Error => self.show_error,
                            };

                            // Apply text filter
                            let matches_filter = if self.filter_text.is_empty() {
                                true
                            } else {
                                entry
                                    .message
                                    .to_lowercase()
                                    .contains(&self.filter_text.to_lowercase())
                            };

                            if show_entry && matches_filter {
                                // Format timestamp
                                let time_since_epoch = entry
                                    .timestamp
                                    .duration_since(SystemTime::UNIX_EPOCH)
                                    .unwrap_or_default();
                                let secs = time_since_epoch.as_secs();
                                let millis = time_since_epoch.subsec_millis();

                                let hours = (secs / 3600) % 24;
                                let minutes = (secs / 60) % 60;
                                let seconds = secs % 60;

                                let timestamp = format!(
                                    "{:02}:{:02}:{:02}.{:03}",
                                    hours, minutes, seconds, millis
                                );

                                ui.horizontal_wrapped(|ui| {
                                    ui.spacing_mut().item_spacing.x = 4.0;
                                    ui.label(RichText::new(&timestamp).color(Color32::GRAY));
                                    ui.label(
                                        RichText::new(entry.level_prefix())
                                            .color(entry.level_color())
                                            .strong(),
                                    );
                                    ui.label(egui::RichText::new(&entry.message))
                                        .on_hover_text(&entry.message);
                                });
                            }
                        }
                    }

                    if self.auto_scroll {
                        scroll_to_bottom = true;
                    }
                });
        });
    }

    pub fn buffer(&self) -> LogBuffer {
        self.buffer.clone()
    }
}

// Implement a tracing subscriber to capture logs from tracing
pub mod tracing_support {
    use super::{LogBuffer, LogEntry, LogLevel};
    use tracing::level_filters::LevelFilter;
    use tracing::Subscriber;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::Registry;
    use tracing_subscriber::Layer;

    pub struct LogBufferLayer {
        buffer: LogBuffer,
    }

    impl LogBufferLayer {
        pub fn new(buffer: LogBuffer) -> Self {
            Self { buffer }
        }
    }

    impl<S> Layer<S> for LogBufferLayer
    where
        S: Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            // Extract information from the event
            let meta = event.metadata();

            // Convert tracing level to our LogLevel
            let level = match *meta.level() {
                tracing::Level::ERROR => LogLevel::Error,
                tracing::Level::WARN => LogLevel::Warning,
                tracing::Level::INFO => LogLevel::Info,
                tracing::Level::DEBUG | tracing::Level::TRACE => LogLevel::Debug,
            };

            // Extract the message
            let mut message = String::new();
            let mut visitor = MessageVisitor(&mut message);
            event.record(&mut visitor);

            // Add to log buffer
            self.buffer.add_entry(LogEntry::new(level, message));
        }
    }

    struct MessageVisitor<'a>(&'a mut String);

    impl<'a> tracing::field::Visit for MessageVisitor<'a> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0.push_str(&format!("{:?}", value));
            }
        }
    }

    pub fn init_tracing(buffer: LogBuffer) -> Result<(), Box<dyn std::error::Error>> {
        let layer = LogBufferLayer::new(buffer);

        let subscriber = Registry::default().with(layer).with(LevelFilter::DEBUG);

        tracing::subscriber::set_global_default(subscriber)?;

        Ok(())
    }
}
