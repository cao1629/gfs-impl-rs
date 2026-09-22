use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

struct TaggedFormat {
    tag: String,
}

impl<S, N> FormatEvent<S, N> for TaggedFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> fmt::Result {
        let letter = match *event.metadata().level() {
            Level::ERROR => 'E',
            Level::WARN => 'W',
            Level::INFO => 'I',
            Level::DEBUG => 'D',
            Level::TRACE => 'T',
        };
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let secs = since_epoch.as_secs();
        write!(
            writer,
            "{} {:02}:{:02}:{:02}.{:03} [{}] ",
            letter,
            (secs / 3600) % 24,
            (secs / 60) % 60,
            secs % 60,
            since_epoch.subsec_millis(),
            self.tag
        )?;
        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

pub fn init(tag: &str) {
    let _ = tracing_subscriber::fmt()
        .event_format(TaggedFormat { tag: tag.to_string() })
        .with_writer(std::io::stderr)
        .with_max_level(Level::INFO)
        .try_init();
}
