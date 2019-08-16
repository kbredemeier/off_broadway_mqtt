# Changelog

## 0.2.0

- Simplifies the API of the Config module:
  - Removes `:default` arg from `Config.new/1`
  - Adds `Config.new_from_app_config/1`
- Does not convert `host` to a chartlist anymore. Tortoise already does that!
  This also fixes the issue with logger metadate including the hostname as a
  charlist.
