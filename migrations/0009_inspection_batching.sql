-- Optional for legacy provider turns; populated after inspection scheduling.
ALTER TABLE usage_turns ADD COLUMN inspection_batch_json TEXT;
