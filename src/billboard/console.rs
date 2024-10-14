use std::borrow::Cow;
use std::error::Error;
use std::io::Cursor;
use std::sync::Arc;

use ab_glyph::{Font, FontArc, Glyph, PxScale, PxScaleFont, ScaleFont};
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use circular_buffer::CircularBuffer;
use image::{ImageBuffer, Rgba};
use imageproc::drawing::{draw_line_segment_mut, draw_text_mut};
use serenity::all::{ChannelId, Colour, CreateAttachment, CreateEmbed, CreateEmbedFooter, EditMessage, Http, RoleId, Timestamp};
use serenity::builder::CreateMessage;
use tokio::sync::mpsc::Receiver;

use crate::billboard::{BillboardLocation};
use crate::bootstrap::Core;

pub const IMAGE_NAME: &str = "console.png";

pub enum ConsoleCommand {
    Print(DateMessage, bool),
    Die
}

pub enum OrderedConsoleCommand {
    Printall(Vec<OrderMessage>, bool), //Display this entire message
    Die
}

/*async fn handle_print(buf: &mut CircularBuffer<10, DateMessage>) -> anyhow::Result<()> {
    
}*/

pub async fn task_console<const N: usize>(core: Arc<Core>, ctx: Arc<Http>, id: &[u8; N], name: &str, mut rx: Receiver<ConsoleCommand>) -> anyhow::Result<()> {
    let mut buf = CircularBuffer::<10, DateMessage>::new();
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ConsoleCommand::Print(message, notify) => {
                const ROLE_KEY: &[u8; 8] = b"role_key";
                let role_u64: u64 = core.discord_db.get(ROLE_KEY)?.map(|v| {
                    return bincode::deserialize::<u64>(v.as_ref())
                }).unwrap_or(Ok(1))?;

                let mention_role: RoleId = RoleId::new(role_u64);


                buf.push_back(message);
                let opt = core.discord_db.get(id).unwrap();
                if opt.is_some() {
                    let frend = generate_console_output(buf.to_vec());
                    let contents = opt.unwrap();

                    let old = bincode::deserialize::<BillboardLocation>(contents.as_ref()).unwrap();
                    let old_channel = ChannelId::new(old.channel_id);
                    let edit = generate_edit(&ctx, name, notify, mention_role, frend, old, old_channel).await;

                    if let Err(why) = edit {
                        eprintln!("Error sending message: {why:?}");
                    };
                }
            }
            ConsoleCommand::Die => {
                break;
            }
        }
    }
    
    Ok(())
}

pub async fn task_ordered_console<const N: usize>(core: Arc<Core>, ctx: Arc<Http>, id: &[u8; N], name: &str, mut rx: Receiver<OrderedConsoleCommand>) -> anyhow::Result<()> {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            OrderedConsoleCommand::Printall(message, notify) => {
                const ROLE_KEY: &[u8; 8] = b"role_key";
                let role_u64: u64 = core.discord_db.get(ROLE_KEY)?.map(|v| {
                    return bincode::deserialize::<u64>(v.as_ref())
                }).unwrap_or(Ok(1))?;
                
                let mention_role: RoleId = RoleId::new(role_u64);

                let opt = core.discord_db.get(id).unwrap();
                if opt.is_some() {
                    let frend = generate_console_output(message);
                    let contents = opt.unwrap();

                    let old = bincode::deserialize::<BillboardLocation>(contents.as_ref()).unwrap();
                    let old_channel = ChannelId::new(old.channel_id);
                    let edit = generate_edit(&ctx, name, notify, mention_role, frend, old, old_channel).await;

                    if let Err(why) = edit {
                        eprintln!("Error sending message: {why:?}");
                    };
                }
            }
            OrderedConsoleCommand::Die => {
                break;
            }
        }
    }

    Ok(())
}

async fn generate_edit<'a>(ctx: &Arc<Http>, name: &str, notify: bool, mention_role: RoleId, frend: Cow<'a,[u8]>, old: BillboardLocation, old_channel: ChannelId) -> anyhow::Result<()> {
    let edit = old_channel.edit_message(
        &ctx,
        u64::from(old.message_id),
        {
            let mut msg = EditMessage::new()
                .remove_all_attachments()
                .new_attachment(CreateAttachment::bytes(frend, IMAGE_NAME))
                .embed(generate_console_embed(true, name));

            if notify {
                msg = msg.content(format!("ALERT: <@{}>", mention_role));
            }

            msg
        }
    ).await;
    Ok(())
}


fn generate_console_embed(online: bool, name: &str) -> CreateEmbed {
    let c = if online {
        Colour::from_rgb(120,255,120)
    } else {
        Colour::from_rgb(255,120,120)
    };

    let embed = CreateEmbed::new()
        .color(c)
        .attachment(IMAGE_NAME)
        .title(if online {format!("STRIDER | {} CONSOLE [ ONLINE ]", name)} else {format!("STRIDER | {} CONSOLE [ OFFLINE ]", name)})
        .footer(CreateEmbedFooter::new("auriium software"))
        .timestamp(Timestamp::now());

    embed
}




// Define the ConsoleMessage struct
#[derive(Clone, Default)]
pub struct ConsoleMessage<K: Ord + Copy> {
    message: String,
    children: Vec<ConsoleMessage<K>>,
    order: K,
}

pub type DateMessage = ConsoleMessage<DateTime<Utc>>;
pub type OrderMessage = ConsoleMessage<u8>;

impl<T: Ord + Copy> ConsoleMessage<T> {
    pub(crate) fn new_full(message: &str, children: Vec<ConsoleMessage<T>>, order: T) -> ConsoleMessage<T> {
        return ConsoleMessage {
            message: message.to_string(),
            children,
            order,
        }
    }

}

impl<T: Ord + Copy + Default> ConsoleMessage<T> {
    
    pub(crate) fn new(s: String) -> Self {
        return ConsoleMessage {
            message: s,
            children: vec![],
            order: T::default()
        }
    }

    pub(crate) fn new_ord(s: String, ord: T) -> Self {
        return ConsoleMessage {
            message: s,
            children: vec![],
            order: ord
        }
    }

    pub(crate) fn new_str(s: &str) -> Self {
        return ConsoleMessage {
            message: s.to_string(),
            children: vec![],
            order: T::default()
        }
    }

    pub(crate) fn new_children(s: String, children: Vec<String>) -> Self {

       let cd: Vec<ConsoleMessage<T>> = children.iter().map(|f| {
           return ConsoleMessage {
               message: f.to_string(),
               children: vec![],
               order: T::default(),
           }
        }).collect();

        return ConsoleMessage {
            message: s,
            children: cd,
            order: T::default()
        }
    }

    pub(crate) fn new_children_ord(s: String, children: Vec<String>, ord: T) -> Self {

        let cd: Vec<ConsoleMessage<T>> = children.iter().map(|f| {
            return ConsoleMessage {
                message: f.to_string(),
                children: vec![],
                order: T::default(),
            }
        }).collect();

        return ConsoleMessage {
            message: s,
            children: cd,
            order: ord
        }
    }
}

const GREEN: Rgba<u8> = Rgba([120u8, 255u8, 120u8, 255u8]);
const BLACK: Rgba<u8> = Rgba([0u8, 0u8, 0u8, 255u8]);
const FONT_SIZE: f32 = 18.0;
const LINE_HEIGHT: u32 = 20;
const BASEDNESS: u32 = 10;
const INDENT_WIDTH: u32 = 14;
const MARGIN_LEFT: u32 = 20;
const MARGIN_TOP: u32 = 20;
const IMAGE_SIZE: u32 = 400;
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
    let uncapped_height = if original_height > IMAGE_SIZE { original_height } else { IMAGE_SIZE };
    let uncapped_width = if original_width > IMAGE_SIZE { original_width } else { IMAGE_SIZE };

    let mut img = ImageBuffer::from_pixel(uncapped_width, uncapped_height, Rgba([0, 0, 0, 255]));

    // Initialize y position
    let mut y_write_pos = MARGIN_TOP;

    // Render messages
    for message in &messages {
        y_write_pos = render_message(message, &font, &mut img, MARGIN_LEFT, y_write_pos, 0);
    }

    // Save the image to a buffer
    let mut buffer = Cursor::new(Vec::new());
    img.write_to(&mut buffer, image::ImageFormat::Png)
        .unwrap();

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
                (line_start_x as f32 + (INDENT_WIDTH - (INDENT_WIDTH / 4)) as f32, child_y as f32),
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

fn compute_max_text_width<T: Ord + Copy>(message: &ConsoleMessage<T>, font: &PxScaleFont<&FontArc>) -> u32 {
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