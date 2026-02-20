-- ============================================
-- NS Rail Disruptions Database Schema
-- Azure SQL (T-SQL) Version
-- ============================================

-- 表1: 原始数据表
CREATE TABLE raw_disruptions (
    id INTEGER IDENTITY(1,1) PRIMARY KEY,
    disruption_id NVARCHAR(100) NOT NULL UNIQUE,
    raw_json NVARCHAR(MAX) NOT NULL,
    fetched_at DATETIME2 DEFAULT GETDATE()
);

-- 表2: 清洗后的数据表
CREATE TABLE disruptions (
    id INTEGER IDENTITY(1,1) PRIMARY KEY,
    disruption_id NVARCHAR(100) NOT NULL UNIQUE,

    type NVARCHAR(50) NOT NULL,
    title NVARCHAR(500),
    description NVARCHAR(MAX),

    start_time DATETIME2,
    end_time DATETIME2,
    duration_minutes FLOAT,

    impact_level INTEGER CHECK(impact_level BETWEEN 1 AND 5),
    affected_stations NVARCHAR(500),

    is_resolved BIT DEFAULT 0,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),

    FOREIGN KEY (disruption_id) REFERENCES raw_disruptions(disruption_id)
);

-- 表3: 车站主数据表
CREATE TABLE stations (
    station_code NVARCHAR(10) PRIMARY KEY,
    station_name NVARCHAR(200) NOT NULL,
    country NVARCHAR(10) DEFAULT 'NL',
    latitude FLOAT,
    longitude FLOAT,
    last_updated DATETIME2 DEFAULT GETDATE()
);

-- 表4: 每日统计表
CREATE TABLE daily_stats (
    date DATE PRIMARY KEY,
    total_disruptions INTEGER DEFAULT 0,
    total_cancellations INTEGER DEFAULT 0,
    avg_duration_minutes FLOAT,
    max_duration_minutes INTEGER,
    most_affected_station NVARCHAR(10),
    peak_hour INTEGER,
    calculated_at DATETIME2 DEFAULT GETDATE()
);

-- ============================================
-- 索引
-- ============================================
CREATE INDEX idx_raw_fetched_at ON raw_disruptions(fetched_at);
CREATE INDEX idx_disruptions_type_resolved ON disruptions(type, is_resolved);
CREATE INDEX idx_disruptions_start_time ON disruptions(start_time);
CREATE INDEX idx_disruptions_impact ON disruptions(impact_level);

-- ============================================
-- 初始化车站数据
-- ============================================
INSERT INTO stations (station_code, station_name, latitude, longitude) VALUES
('ASD', 'Amsterdam Centraal', 52.3791, 4.9003),
('UTR', 'Utrecht Centraal', 52.0894, 5.1101),
('RTD', 'Rotterdam Centraal', 51.9249, 4.4690),
('EHV', 'Eindhoven Centraal', 51.4433, 5.4814),
('GVC', 'Den Haag Centraal', 52.0808, 4.3247),
('LEDN', 'Leiden Centraal', 52.1664, 4.4817);