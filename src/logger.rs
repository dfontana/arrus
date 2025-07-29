#[derive(Clone)]
pub struct Logger {
    prefix: String,
}

impl Logger {
    pub fn new(component: &str) -> Self {
        Self {
            prefix: format!(
                "[{}arRPC{} > {}{}{}]",
                Self::rgb(88, 101, 242, ""),
                Self::reset_color(),
                Self::rgb(87, 242, 135, ""),
                component,
                Self::reset_color()
            ),
        }
    }

    pub fn info(&self, message: &str) {
        println!("{} {}", self.prefix, message);
    }

    pub fn error(&self, message: &str) {
        eprintln!("{} ERROR: {}", self.prefix, message);
    }

    fn rgb(r: u8, g: u8, b: u8, text: &str) -> String {
        format!("\x1b[38;2;{};{};{}m{}", r, g, b, text)
    }

    fn reset_color() -> &'static str {
        "\x1b[0m"
    }
}
