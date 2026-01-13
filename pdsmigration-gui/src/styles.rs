use egui::{RichText, Theme};

/// Margin to be applied to the main frame of the application.
pub const FRAME_MARGIN: f32 = 50.0;

/// Corner radius for the input fields.
pub const INPUT_CORNER_RADIUS: u8 = 6;

/// Background color for the UI.
pub const FRAME_BG_COLOR: egui::Color32 = egui::Color32::from_rgb(250, 250, 250);

/// Background color for the UI.
pub const FRAME_BG_DARK_COLOR: egui::Color32 = egui::Color32::from_rgb(250, 250, 250);

/// Text color for the UI.
pub const FRAME_TEXT_COLOR: egui::Color32 = egui::Color32::from_rgb(31, 11, 53);

/// Size of the subtitle text.
pub const SUBTITLE_SIZE: f32 = 24.0;

/// Background color for the buttons.
pub const BUTTON_BG_COLOR: egui::Color32 = egui::Color32::from_rgb(42, 255, 186);

/// Input field width.
pub const INPUT_WIDTH: f32 = 200.0;

/// Base measure to be used for different spacing calculations in the UI.
pub const WIDGET_SPACING_BASE: f32 = 5.0;

/// Font name for the main UI font.
const MAIN_FONT_NAME: &str = "Geist";

/// Returns a frame with styles applied to be used as the main application frame.
pub fn get_styled_frame(ctx: &egui::Context) -> egui::Frame {
    egui::Frame::canvas(ctx.style().as_ref())
    // .inner_margin(egui::vec2(FRAME_MARGIN, FRAME_MARGIN))
    // .fill(FRAME_BG_COLOR)
}

/// Sets up the fonts for the application using the `egui` context.
pub fn setup_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    fonts.font_data.insert(
        MAIN_FONT_NAME.to_owned(),
        egui::FontData::from_static(include_bytes!("../assets/Geist-VariableFont_wght.ttf")).into(),
    );

    fonts
        .families
        .entry(egui::FontFamily::Proportional)
        .or_default()
        .insert(0, MAIN_FONT_NAME.to_owned());

    ctx.set_fonts(fonts);
}

/// Sets the UI text color.
pub fn set_text_color(_ui: &mut egui::Ui) {
    // ui.visuals_mut().override_text_color = Some(FRAME_TEXT_COLOR);
}

/// Renders a subtitle-styled label with a specific text.
pub fn render_subtitle(ui: &mut egui::Ui, ctx: &egui::Context, text: &str) {
    render_heading(ui, ctx, text, SUBTITLE_SIZE);
}

/// Renders a styled input field with a given label and control text.
pub fn render_input(
    ui: &mut egui::Ui,
    label: &str,
    text: &mut String,
    is_password: bool,
    text_hint: Option<&str>,
) {
    ui.add_space(WIDGET_SPACING_BASE);
    ui.label(RichText::new(label));

    let mut edit_text = egui::TextEdit::singleline(text)
        .password(is_password)
        .desired_width(INPUT_WIDTH);
    if let Some(hint) = text_hint {
        let hint_text = "ex: ".to_string() + hint;
        edit_text = edit_text.hint_text(hint_text);
    }
    ui.add(edit_text);
    ui.add_space(WIDGET_SPACING_BASE);
}

/// Renders a styled input field with a given label and control text.
pub fn render_input_disabled(
    ui: &mut egui::Ui,
    label: &str,
    text: &mut String,
    is_password: bool,
    text_hint: Option<&str>,
) {
    ui.add_space(WIDGET_SPACING_BASE);
    ui.label(RichText::new(label));

    let mut edit_text = egui::TextEdit::singleline(text)
        .password(is_password)
        .desired_width(INPUT_WIDTH)
        .interactive(false);
    if let Some(hint) = text_hint {
        edit_text = edit_text.hint_text(hint);
    }
    ui.add(edit_text);
    ui.add_space(WIDGET_SPACING_BASE);
}

pub fn render_button(ui: &mut egui::Ui, ctx: &egui::Context, label: &str, callback: impl FnOnce()) {
    let theme = ctx.theme();

    ui.spacing_mut().button_padding =
        egui::vec2(4.0 * WIDGET_SPACING_BASE, 2.0 * WIDGET_SPACING_BASE);

    let text_label = match theme {
        Theme::Dark => RichText::new(label),
        Theme::Light => RichText::new(label).color(FRAME_TEXT_COLOR),
    };
    let button = match theme {
        Theme::Dark => egui::Button::new(text_label),
        Theme::Light => egui::Button::new(text_label).fill(BUTTON_BG_COLOR),
    };

    if ui.add(button).clicked() {
        callback();
    }
}

/// Renders a heading-styled label with a specific text and size.
fn render_heading(ui: &mut egui::Ui, ctx: &egui::Context, text: &str, size: f32) {
    match ctx.theme() {
        Theme::Dark => {
            egui::Frame::default()
                .inner_margin(egui::vec2(size / 2.0, size / 2.0))
                .show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.label(
                            RichText::new(text)
                                .text_style(egui::TextStyle::Heading)
                                .size(size)
                                .strong(),
                        );
                    });
                });
        }
        Theme::Light => {
            egui::Frame::default()
                .inner_margin(egui::vec2(size / 2.0, size / 2.0))
                .show(ui, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.label(
                            RichText::new(text)
                                .text_style(egui::TextStyle::Heading)
                                .size(size)
                                .color(FRAME_TEXT_COLOR)
                                .strong(),
                        );
                    });
                });
        }
    }
}
