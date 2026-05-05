"""OCR keyword definitions shared across processing and app layers."""

KW_KEYS: list[str] = [
    "conservation_restriction",
    "article_97",
    "deed_restriction",
    "chapter_61",
    "agricultural_preservation_restriction",
    "perpetual_restriction",
    "ccr",
]

KW_LABELS: dict[str, str] = {
    "conservation_restriction":             "Conservation Restriction",
    "article_97":                           "Article 97",
    "deed_restriction":                     "Deed Restriction",
    "chapter_61":                           "Chapter 61",
    "agricultural_preservation_restriction": "Ag. Preservation Restriction",
    "perpetual_restriction":                "Perpetual Restriction",
    "ccr":                                  "CC&R",
}
