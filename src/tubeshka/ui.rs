//! Settings → "Шлюз TGeshka": the key, the gateway's state, the update channel and "What's new".
use super::{config, gateway, proxy};
use crate::ui::*;
use gpui_kit::component::input::{Input, InputEvent, InputState};
use gpui_kit::*;
use std::sync::atomic::Ordering;

pub struct GatewayPanel {
    key: Entity<InputState>,
    url: Entity<InputState>,
    status: Option<Result<gateway::Info, String>>,
    checking: bool,
    _subs: Vec<Subscription>,
}

impl GatewayPanel {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let key = cx.new(|cx| InputState::new(window, cx).placeholder("Ключ TGeshka (tc_…) или ссылка tgeshka://gateway?…"));
        let url = cx.new(|cx| {
            let mut i = InputState::new(window, cx).placeholder("https://…");
            i.set_value(config::get().gateway_url, window, cx);
            i
        });
        let mut subs = Vec::new();
        subs.push(cx.subscribe_in(&key, window, |this: &mut Self, st, ev: &InputEvent, window, cx| {
            if !matches!(ev, InputEvent::PressEnter { .. } | InputEvent::Blur) {
                return;
            }
            let v = st.read(cx).value().trim().to_string();
            if v.is_empty() {
                return;
            }
            let (link_url, token) = match config::parse_link(&v) {
                Some((u, t)) => (u, t),
                None => (None, Some(v)),
            };
            match token.filter(|t| config::valid_token(t)) {
                Some(t) => {
                    config::update(|c| {
                        c.token = t;
                        if let Some(u) = link_url {
                            c.gateway_url = u;
                        }
                    });
                    st.update(cx, |i, cx| i.set_value("", window, cx));
                    toast(cx, "Ключ сохранён");
                    this.check(cx);
                }
                None => toast(cx, "Это не ключ TGeshka: он начинается с tc_ и длиной 46 знаков"),
            }
        }));
        subs.push(cx.subscribe_in(&url, window, |this: &mut Self, st, ev: &InputEvent, _, cx| {
            if matches!(ev, InputEvent::PressEnter { .. } | InputEvent::Blur) {
                let v = st.read(cx).value().trim().to_string();
                if v != config::get().gateway_url {
                    config::update(|c| c.gateway_url = v);
                    this.check(cx);
                }
            }
        }));
        let mut me = Self { key, url, status: None, checking: false, _subs: subs };
        me.check(cx);
        me
    }

    fn check(&mut self, cx: &mut Context<Self>) {
        if self.checking {
            return;
        }
        self.checking = true;
        cx.notify();
        cx.spawn(async move |this, cx| {
            let res = blocking(gateway::info).await.map_err(|e| e.to_string());
            let _ = this.update(cx, |me, cx| {
                me.checking = false;
                me.status = Some(res);
                cx.notify();
            });
        })
        .detach();
    }
}

fn row(title: &str, desc: &str, control: impl IntoElement, cx: &App) -> Div {
    let p = pal(cx);
    div()
        .flex()
        .items_center()
        .justify_between()
        .flex_wrap()
        .gap_x_6()
        .gap_y_2()
        .py_3()
        .border_b_1()
        .border_color(p.border)
        .child(
            div()
                .flex()
                .flex_col()
                .flex_1()
                .min_w(px(240.))
                .gap_0p5()
                .child(div().text_sm().font_weight(FontWeight::MEDIUM).text_color(p.text).child(title.to_string()))
                .when(!desc.is_empty(), |d| d.child(div().text_xs().text_color(p.text2).child(desc.to_string()))),
        )
        .child(div().flex_none().child(control))
}

fn field(st: &Entity<InputState>, w: f32, cx: &App) -> Div {
    let p = pal(cx);
    div().w(px(w)).px_3().py_1().rounded_lg().border_1().border_color(p.border).bg(p.bg).child(Input::new(st).appearance(false))
}

fn mb(b: u64) -> String {
    if b >= 1 << 30 { format!("{:.2} ГБ", b as f64 / (1u64 << 30) as f64) } else { format!("{:.1} МБ", b as f64 / (1u64 << 20) as f64) }
}

impl Render for GatewayPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let p = pal(cx);
        let c = config::get();
        let st = proxy::stats();
        let key_desc = if config::valid_token(&c.token) {
            format!("Сохранён ключ {}… Вставьте новый, чтобы заменить", &c.token[..7])
        } else {
            "Тот же ключ, что в TGeshka. Без него видео и страницы не загрузятся".to_string()
        };
        let state = match (&self.status, self.checking) {
            (_, true) => "Проверяю…".to_string(),
            (Some(Ok(i)), _) => {
                let who = if i.description.is_empty() { format!("ключ {}", i.id) } else { format!("ключ «{}»", i.description) };
                let mut s = format!("Подключено: {who}, ответ шлюза {} мс", i.rtt_ms);
                if !i.proxy {
                    s.push_str(". Шлюз пока не пускает клиент YouTube");
                }
                if i.upgrade_required {
                    s.push_str(". Нужно обновить приложение");
                }
                s
            }
            (Some(Err(e)), _) => format!("Нет связи: {e}"),
            (None, _) => String::new(),
        };
        let traffic = format!(
            "Открыто соединений: {}, всего через шлюз: {}, скачано {}{}",
            st.active.load(Ordering::Relaxed),
            st.opened.load(Ordering::Relaxed),
            mb(st.down.load(Ordering::Relaxed)),
            proxy::last_error().map(|e| format!(". Последняя ошибка: {e}")).unwrap_or_default()
        );
        let channel = super::update::channel();
        let chan_btn = |id: &'static str, val: &'static str, label: &'static str| {
            let active = channel == val;
            div()
                .id(id)
                .px_3()
                .py_1()
                .rounded_lg()
                .cursor_pointer()
                .text_sm()
                .when(active, |d| d.bg(p.accent).text_color(white()))
                .when(!active, |d| d.bg(p.chip).text_color(p.text))
                .child(label)
                .on_click(move |_, _, cx| {
                    config::update(|c| c.channel = val.to_string());
                    store_kv_reset_skip();
                    cx.refresh_windows();
                })
        };
        let mut card = div()
            .flex()
            .flex_col()
            .rounded_xl()
            .bg(p.surface)
            .px_5()
            .py_2()
            .child(row(
                "Весь трафик через шлюз TGeshka",
                "YouTube открывается с адреса за границей. Выключать — только если есть свой способ. Нужен перезапуск",
                div()
                    .id("gw-on")
                    .relative()
                    .w(px(40.))
                    .h(px(22.))
                    .rounded_full()
                    .cursor_pointer()
                    .bg(if c.enabled { p.accent } else { p.hover })
                    .child(div().absolute().top(px(3.)).left(if c.enabled { px(21.) } else { px(3.) }).size(px(16.)).rounded_full().bg(white()))
                    .on_click(|_, _, cx| {
                        config::update(|c| c.enabled = !c.enabled);
                        toast(cx, "Перезапустите приложение, чтобы применить");
                        cx.refresh_windows();
                    }),
                cx,
            ))
            .child(row("Ключ", &key_desc, field(&self.key, 360., cx), cx));
        if super::built_in_gateway().is_empty() {
            card = card.child(row("Адрес шлюза", "Его присылают вместе с ключом", field(&self.url, 360., cx), cx));
        }
        card = card
            .child(row(
                "Состояние",
                &state,
                pill("gw-check", Some("refresh-cw"), "Проверить", cx).on_click(cx.listener(|this, _, _, cx| this.check(cx))),
                cx,
            ))
            .child(row("Трафик", &traffic, div(), cx))
            .child(row(
                "Канал обновлений",
                "Бета получает новые версии раньше, в том числе свежие правки основного проекта",
                div().flex().gap_1().child(chan_btn("ch-stable", "stable", "Стабильный")).child(chan_btn("ch-beta", "beta", "Бета")),
                cx,
            ));
        let mut news = div().flex().flex_col().rounded_xl().bg(p.surface).px_5().py_3().gap_3().child(
            div().text_sm().font_weight(FontWeight::SEMIBOLD).text_color(p.text).child(format!("Что нового · версия {}", super::version())),
        );
        for ch in super::changelog().into_iter().take(6) {
            let mut block = div()
                .flex()
                .flex_col()
                .gap_1()
                .child(div().text_xs().font_weight(FontWeight::SEMIBOLD).text_color(p.text2).child(format!("{} · {}", ch.version, ch.date)));
            for it in ch.items {
                block = block.child(div().text_sm().text_color(p.text).child(format!("• {it}")));
            }
            news = news.child(block);
        }
        div().flex().flex_col().gap_4().child(card).child(news)
    }
}

/// A skipped version may be offered again after the channel changes.
fn store_kv_reset_skip() {
    crate::store::kv_set("update_skip", "");
}
