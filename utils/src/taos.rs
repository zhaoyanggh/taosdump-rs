use libtaos::*;
use std::env::var;
fn var_or_default(env: &str, default: &str) -> String {
    var(env).unwrap_or(default.to_string())
}

pub fn taos_connect() -> Result<Taos, Error> {
    TaosCfgBuilder::default()
        .ip(&var_or_default("TEST_TAOS_IP", "127.0.0.1"))
        .user(&var_or_default("TEST_TAOS_USER", "root"))
        .pass(&var_or_default("TEST_TAOS_PASS", "taosdata"))
        .db(&var_or_default("TEST_TAOS_DB", "log"))
        .port(
            var_or_default("TEST_TAOS_PORT", "6030")
                .parse::<u16>()
                .unwrap(),
        )
        .build()
        .expect("TaosCfg builder error")
        .connect()
}
