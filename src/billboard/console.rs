use std::borrow::Cow;
use std::io::Cursor;
use std::sync::Arc;

use ab_glyph::{Font, FontArc, Glyph, PxScale, PxScaleFont, ScaleFont};
use chrono::{DateTime, Utc};
use circular_buffer::CircularBuffer;
use image::{ImageBuffer, Rgba};
use imageproc::drawing::{draw_line_segment_mut, draw_text_mut};
use serenity::all::{ChannelId, Colour, CreateAttachment, CreateEmbed, CreateEmbedFooter, EditMessage, Http, MessageId, Timestamp};
use tokio::sync::mpsc::Receiver;

use crate::billboard::BillboardLocation;
use crate::core::Core;
use crate::core::database::SignpostDocument;

// TODO: this code needs to be rewritten to not use pngs for displaying text, lol
pub const IMAGE_NAME: &str = "console.png";

#[derive(Clone, Default)]
pub struct ConsoleMessage<K: Ord + Copy> {
    message: String,
    children: Vec<ConsoleMessage<K>>,
    order: K,
}
pub type DateCommand = ConsoleCommand<DateTime<Utc>>;
pub type OrderCommand = ConsoleCommand<u8>;
pub enum ConsoleCommand<K: Ord + Copy> {
    Tick,
    Print(ConsoleMessage<K>, bool),
    PrintAll(Vec<ConsoleMessage<K>>, bool),
    Die,
}
pub struct Console<U: Ord + Copy> {
    id: &'static str,
    name: &'static str,
    core: Arc<Core>,
    ctx: Arc<Http>,
    rx: Receiver<ConsoleCommand<U>>,
}

impl<K: Ord + Copy> Console<K> {
    pub fn new(id: &'static str, name: &'static str, core: Arc<Core>, ctx: Arc<Http>, rx: Receiver<ConsoleCommand<K>>) -> Self {
        Self { id, name, core, ctx, rx }
    }
}

impl Console<DateTime<Utc>> {
    pub async fn task(mut self) -> anyhow::Result<()> {
        let mut buf = CircularBuffer::<17, ConsoleMessage<DateTime<Utc>>>::new();
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                ConsoleCommand::Tick => {
                    let opt = self.core.db.get_signpost(self.id.to_string()).await?;
                    if opt.is_some() {
                        let frend = generate_console_output(buf.to_vec());
                        let contents = opt.unwrap();
                        let old_channel = ChannelId::new(contents.channel_id.parse()?);

                        let edit =
                            generate_edit(&self.ctx, self.name, false, frend, contents, old_channel).await;

                        if let Err(why) = edit {
                            eprintln!("Error sending message: {why:?}");
                        };
                    }
                }
                ConsoleCommand::Print(message, notify) => {
                    /* const ROLE_KEY: &[u8; 8] = b"role_key";
                    let role_u64: u64 = core.discord_db.get(ROLE_KEY)?.map(|v| {
                        return bincode::deserialize::<u64>(v.as_ref())
                    }).unwrap_or(Ok(1))?;

                    let mention_role: RoleId = RoleId::new(role_u64);*/
                    buf.push_back(message);
                }
                ConsoleCommand::Die => {
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }
}

impl Console<u8> {

    pub async fn task_ord<const N: usize>() -> anyhow::Result<()> {
        Ok(())
    }
    
    
    pub async fn task_ordered_console<const N: usize>(
        self,
        core: Arc<Core>,
        ctx: Arc<Http>,
        mut rx: Receiver<OrderCommand>,
    ) -> anyhow::Result<()> {
        let mut storage: Vec<ConsoleMessage<u8>> = vec![];

        while let Some(cmd) = rx.recv().await {
            match cmd {
                OrderCommand::Tick => {
                    let opt = self.core.db.get_signpost(self.id.to_string()).await?;
                    if opt.is_some() {
                        let frend = generate_console_output(storage.clone());
                        let contents = opt.unwrap();
                        let old_channel = ChannelId::new(contents.channel_id.parse()?);
                        let edit =
                            generate_edit(&ctx, self.name, false, frend, contents, old_channel).await;

                        if let Err(why) = edit {
                            eprintln!("Error sending message: {why:?}");
                        };
                    }
                }
                OrderCommand::Print(a, ..) => {}
                OrderCommand::PrintAll(message, notify) => {
                    storage = message;
                }
                OrderCommand::Die => {
                    break;
                }
            }
        }

        Ok(())
    }
}

//TODO make this performant and not graphical
async fn generate_edit<'a>(
    ctx: &Arc<Http>,
    name: &str,
    notify: bool,
    frend: Cow<'a, [u8]>,
    old: SignpostDocument,
    old_channel: ChannelId,
) -> anyhow::Result<()> {
    let id: MessageId = MessageId::new(old.message_id.parse()?);
    
    let edit = old_channel
        .edit_message(&ctx, id, {
            let msg = EditMessage::new()
                .remove_all_attachments()
                .new_attachment(CreateAttachment::bytes(frend, IMAGE_NAME))
                .embed(generate_console_embed(true, name));

            /*if notify {
                msg = msg.content(format!("ALERT: <@{}>", mention_role));
            }*/

            msg
        })
        .await?;
    Ok(())
}


fn generate_console_embed(online: bool, name: &str) -> CreateEmbed {
    let c = if online {
        Colour::from_rgb(120, 255, 120)
    } else {
        Colour::from_rgb(255, 120, 120)
    };

    let embed = CreateEmbed::new()
        .color(c)
        .attachment(IMAGE_NAME)
        .title(if online {
            format!("STRIDER | {} CONSOLE [ ONLINE ]", name)
        } else {
            format!("STRIDER | {} CONSOLE [ OFFLINE ]", name)
        })
        .footer(CreateEmbedFooter::new("auriium software"))
        .timestamp(Timestamp::now());

    embed
}

impl<T: Ord + Copy> ConsoleMessage<T> {
    pub(crate) fn new_full(
        message: &str,
        children: Vec<ConsoleMessage<T>>,
        order: T,
    ) -> ConsoleMessage<T> {
        return ConsoleMessage {
            message: message.to_string(),
            children,
            order,
        };
    }
}

impl<T: Ord + Copy + Default> ConsoleMessage<T> {
    pub(crate) fn new(s: String) -> Self {
        return ConsoleMessage {
            message: s,
            children: vec![],
            order: T::default(),
        };
    }

    pub(crate) fn new_ord(s: String, ord: T) -> Self {
        return ConsoleMessage {
            message: s,
            children: vec![],
            order: ord,
        };
    }

    pub(crate) fn new_str(s: &str) -> Self {
        return ConsoleMessage {
            message: s.to_string(),
            children: vec![],
            order: T::default(),
        };
    }

    pub(crate) fn new_children(s: String, children: Vec<String>) -> Self {
        let cd: Vec<ConsoleMessage<T>> = children
            .iter()
            .map(|f| {
                return ConsoleMessage {
                    message: f.to_string(),
                    children: vec![],
                    order: T::default(),
                };
            })
            .collect();

        return ConsoleMessage {
            message: s,
            children: cd,
            order: T::default(),
        };
    }

    pub(crate) fn new_children_ord(s: String, children: Vec<String>, ord: T) -> Self {
        let cd: Vec<ConsoleMessage<T>> = children
            .iter()
            .map(|f| {
                return ConsoleMessage {
                    message: f.to_string(),
                    children: vec![],
                    order: T::default(),
                };
            })
            .collect();

        return ConsoleMessage {
            message: s,
            children: cd,
            order: ord,
        };
    }
}

//"pretty" console - please get rid of this

const GREEN: Rgba<u8> = Rgba([120u8, 255u8, 120u8, 255u8]);
const BLACK: Rgba<u8> = Rgba([0u8, 0u8, 0u8, 255u8]);
const FONT_SIZE: f32 = 15.0;
const LINE_HEIGHT: u32 = 20;
const BASEDNESS: u32 = 10;
const INDENT_WIDTH: u32 = 14;
const MARGIN_LEFT: u32 = 20;
const MARGIN_TOP: u32 = 20;
const IMAGE_SIZE: u32 = 600;
const FONT_DATA: &[u8] = include_bytes!("../../assets/VGA.ttf");

fn generate_console_output<T: Ord + Copy>(messages: Vec<ConsoleMessage<T>>) -> Cow<'static, [u8]> {
    let f1 = FontArc::try_from_slice(FONT_DATA).unwrap();
    let font = f1.as_scaled(PxScale::from(FONT_SIZE));

    let mut messages = messages;
    messages.sort_by_key(|m| m.order);

    let mut total_lines = 0;
    let mut max_indent = 0;
    let mut max_text_width = 0;

    for message in &messages {
        total_lines += count_lines(message);
        let indent = max_indent_level(message, 0);
        if indent > max_indent {
            max_indent = indent;
        }
        let width = compute_max_text_width(message, &font);
        if width > max_text_width {
            max_text_width = width;
        }
    }

    // Compute image dimensions
    let original_width = MARGIN_LEFT * 2 + (max_indent + 1) * INDENT_WIDTH + max_text_width;
    let original_height = MARGIN_TOP * 2 + total_lines * LINE_HEIGHT;
    let uncapped_height = if original_height > IMAGE_SIZE {
        original_height
    } else {
        IMAGE_SIZE
    };
    let uncapped_width = if original_width > IMAGE_SIZE {
        original_width
    } else {
        IMAGE_SIZE
    };

    let mut img = ImageBuffer::from_pixel(uncapped_width, uncapped_height, Rgba([0, 0, 0, 255]));

    // Initialize y position
    let mut y_write_pos = MARGIN_TOP;

    // Render messages
    for message in &messages {
        y_write_pos = render_message(message, &font, &mut img, MARGIN_LEFT, y_write_pos, 0);
    }

    // Save the image to a buffer
    let mut buffer = Cursor::new(Vec::new());
    img.write_to(&mut buffer, image::ImageFormat::Png).unwrap();

    // Return the image bytes
    Cow::Owned(buffer.into_inner())
}

// Helper function to render messages
fn render_message<T: Ord + Copy>(
    message: &ConsoleMessage<T>,
    font: &PxScaleFont<&FontArc>,
    img: &mut ImageBuffer<Rgba<u8>, Vec<u8>>,
    x: u32,
    y: u32,
    indent_level: u32,
) -> u32 {
    let pos_x = x + indent_level * INDENT_WIDTH + BASEDNESS;
    let pos_y = y;

    // Draw the message text
    draw_text(img, font, pos_x, pos_y, &message.message);

    let mut new_y = pos_y + LINE_HEIGHT;

    // If the message has children, draw connecting lines
    if !message.children.is_empty() {
        let line_start_x = pos_x;
        let line_start_y = pos_y + (FONT_SIZE) as u32;

        let mut child_positions = Vec::new();

        for child in &message.children {
            let child_start_y = new_y;
            new_y = render_message(child, font, img, x, new_y, indent_level + 1);
            child_positions.push(child_start_y + (FONT_SIZE / 2.0) as u32);
        }

        let line_end_y = *child_positions.last().unwrap();

        // Draw vertical line from parent to last child
        draw_line_segment_mut(
            img,
            (line_start_x as f32, line_start_y as f32),
            (line_start_x as f32, line_end_y as f32),
            GREEN,
        );

        // Draw horizontal lines to each child
        for &child_y in &child_positions {
            draw_line_segment_mut(
                img,
                (line_start_x as f32, child_y as f32),
                (
                    line_start_x as f32 + (INDENT_WIDTH - (INDENT_WIDTH / 4)) as f32,
                    child_y as f32,
                ),
                GREEN,
            );
        }
    }

    new_y
}

fn draw_text(
    img: &mut ImageBuffer<Rgba<u8>, Vec<u8>>,
    font: &PxScaleFont<&FontArc>,
    x: u32,
    y: u32,
    text: &str,
) {
    let scale = PxScale::from(FONT_SIZE);
    let color = Rgba([120u8, 255u8, 120u8, 255u8]);

    draw_text_mut(img, color, x as i32, y as i32, scale, font.font, text);
}

// Function to count total lines
fn count_lines<T: Ord + Copy>(message: &ConsoleMessage<T>) -> u32 {
    let mut lines = 1;
    for child in &message.children {
        lines += count_lines(child);
    }
    lines
}

fn max_indent_level<T: Ord + Copy>(message: &ConsoleMessage<T>, current_level: u32) -> u32 {
    let mut max_level = current_level;
    for child in &message.children {
        let child_level = max_indent_level(child, current_level + 1);
        if child_level > max_level {
            max_level = child_level;
        }
    }
    max_level
}

fn compute_max_text_width<T: Ord + Copy>(
    message: &ConsoleMessage<T>,
    font: &PxScaleFont<&FontArc>,
) -> u32 {
    let mut max_width = text_width(font, &message.message);
    for child in &message.children {
        let width = compute_max_text_width(child, font);
        if width > max_width {
            max_width = width;
        }
    }
    max_width
}

fn text_width(font: &PxScaleFont<&FontArc>, text: &str) -> u32 {
    let scale = PxScale::from(FONT_SIZE);
    let mut width: f32 = 0.0;
    let mut last_glyph_id = None;

    for c in text.chars() {
        if c.is_control() {
            continue;
        }
        let glyph_id = font.glyph_id(c);
        let glyph: Glyph = font.glyph_id(c).with_scale(scale);

        if let Some(last_id) = last_glyph_id {
            width += font.kern(last_id, glyph.id);
        }

        width += font.h_advance(glyph_id);
        last_glyph_id = Some(glyph.id);
    }

    width.ceil() as u32
}
