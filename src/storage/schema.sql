-- ============================================
-- NS Rail Disruptions Database Schema
-- ============================================

-- 表1: 原始数据表
CREATE TABLE IF NOT EXISTS raw_disruptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    disruption_id TEXT NOT NULL UNIQUE,
    raw_json TEXT NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    -- ❌ 删除这里的 INDEX 行
);

-- 表2: 清洗后的数据表
CREATE TABLE IF NOT EXISTS disruptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    disruption_id TEXT NOT NULL UNIQUE,
    
    type TEXT NOT NULL,
    title TEXT,
    description TEXT,
    
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_minutes INTEGER,
    
    impact_level INTEGER CHECK(impact_level BETWEEN 1 AND 5),
    affected_stations TEXT,
    
    is_resolved BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (disruption_id) REFERENCES raw_disruptions(disruption_id)
    -- ❌ 删除这里的 INDEX 行
);

-- 表3: 车站主数据表
CREATE TABLE IF NOT EXISTS stations (
    station_code TEXT PRIMARY KEY,
    station_name TEXT NOT NULL,
    country TEXT DEFAULT 'NL',
    latitude REAL,
    longitude REAL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 表4: 每日统计表
CREATE TABLE IF NOT EXISTS daily_stats (
    date DATE PRIMARY KEY,
    total_disruptions INTEGER DEFAULT 0,
    total_cancellations INTEGER DEFAULT 0,
    avg_duration_minutes REAL,
    max_duration_minutes INTEGER,
    most_affected_station TEXT,
    peak_hour INTEGER,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 索引创建（必须在表创建之后）
-- ============================================

-- 为 raw_disruptions 表创建索引
CREATE INDEX IF NOT EXISTS idx_raw_fetched_at 
    ON raw_disruptions(fetched_at);

-- 为 disruptions 表创建索引
CREATE INDEX IF NOT EXISTS idx_disruptions_type_resolved 
    ON disruptions(type, is_resolved);

CREATE INDEX IF NOT EXISTS idx_disruptions_start_time 
    ON disruptions(start_time);

CREATE INDEX IF NOT EXISTS idx_disruptions_impact 
    ON disruptions(impact_level);

-- ============================================
-- 初始化车站数据
-- ============================================
INSERT OR IGNORE INTO stations (station_code, station_name, latitude, longitude) VALUES
('ASD', 'Amsterdam Centraal', 52.3791, 4.9003),
('UTR', 'Utrecht Centraal', 52.0894, 5.1101),
('RTD', 'Rotterdam Centraal', 51.9249, 4.4690),
('EHV', 'Eindhoven Centraal', 51.4433, 5.4814),
('GVC', 'Den Haag Centraal', 52.0808, 4.3247),
('LEDN', 'Leiden Centraal', 52.1664, 4.4817);

-- ============================================
-- 视图：方便查询
-- ============================================

-- 当前活跃的延误
CREATE VIEW IF NOT EXISTS active_disruptions AS
SELECT 
    d.disruption_id,
    d.type,
    d.title,
    d.start_time,
    d.end_time,
    d.duration_minutes,
    d.impact_level,
    CAST((julianday(d.end_time) - julianday('now')) * 1440 AS INTEGER) as remaining_minutes
FROM disruptions d
WHERE d.is_resolved = 0
  AND d.end_time > datetime('now')
ORDER BY d.impact_level DESC, d.start_time ASC;

-- 车站延误统计
CREATE VIEW IF NOT EXISTS station_disruption_stats AS
SELECT 
    s.station_code,
    s.station_name,
    COUNT(DISTINCT d.disruption_id) as disruption_count,
    AVG(d.duration_minutes) as avg_delay_minutes
FROM stations s
LEFT JOIN disruptions d 
    ON d.affected_stations LIKE '%' || s.station_code || '%'
GROUP BY s.station_code, s.station_name
ORDER BY disruption_count DESC;