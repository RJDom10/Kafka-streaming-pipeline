-- =============================================================================
-- Inicialización de Esquema: Wikimedia Recent Changes
-- Ejecutado automáticamente al levantar el contenedor de PostgreSQL
-- =============================================================================

CREATE TABLE IF NOT EXISTS wiki_recent_changes (
    id SERIAL PRIMARY KEY,
    wiki VARCHAR(50) NOT NULL,
    title TEXT,
    user_name VARCHAR(255),
    bot BOOLEAN DEFAULT FALSE,
    change_type VARCHAR(50),
    length_old INT,
    length_new INT,
    byte_diff INT,
    event_timestamp TIMESTAMPTZ,
    received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Índices para optimizar consultas de laboratorio
CREATE INDEX IF NOT EXISTS idx_wiki_timestamp ON wiki_recent_changes (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_wiki_bot ON wiki_recent_changes (bot);
CREATE INDEX IF NOT EXISTS idx_wiki_lang ON wiki_recent_changes (wiki);
