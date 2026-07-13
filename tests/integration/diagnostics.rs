const CODES: &[&str] = &[
    "FE_SOURCE_NOT_FOUND",
    "FE_DEST_EXISTS",
    "FE_CANCELLED",
    "FE_NO_SPACE",
    "FE_PERMISSION_DENIED",
    "FE_IO",
    "FE_UNKNOWN_COMPRESS_FORMAT",
    "FE_GZIP_REQUIRES_FILE",
];

#[test]
fn errors_toml_parses_and_covers_every_code() {
    let raw = include_str!("../../errors.toml");
    let catalog = error_engine::Catalog::from_str(raw)
        .expect("errors.toml must be valid TOML for error-engine");

    // `Catalog` has no `contains`/lookup method — `render()` is the only
    // way to probe an entry, and it never fails, falling back to
    // `UNKNOWN_CODE` ("[UNK-000] unknown diagnostic code '...'") for a
    // missing entry. Absence of that marker is coverage proof.
    for code in CODES {
        let rendered = catalog.render(code, &[]);
        assert!(
            !rendered.contains(error_engine::UNKNOWN_CODE),
            "errors.toml is missing an entry for {code} (got: {rendered})"
        );
    }
}
