-- ============================================================================
-- AMBIENT.AI GTM PIPELINE - DATABASE SCHEMA
-- ============================================================================
-- PostgreSQL schema for storing leads, enrichments, campaigns, and interactions
-- 
-- Usage:
--   psql -d your_database -f schema.sql
--
-- Or run in Supabase SQL editor
-- ============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- DISTRICTS TABLE
-- Raw scraped district data
-- ============================================================================
CREATE TABLE IF NOT EXISTS districts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    district_name VARCHAR(255) NOT NULL,
    domain VARCHAR(255),
    website VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(2) DEFAULT 'TX',
    enrollment INTEGER,
    tier VARCHAR(50), -- 'Tier 1 - Sweet Spot', 'Tier 2 - Mid-size', etc.
    
    -- Metadata
    scraped_at TIMESTAMP DEFAULT NOW(),
    source VARCHAR(50), -- 'tribune', 'wikipedia', 'manual'
    
    UNIQUE(domain)
);

-- Index for fast lookups
CREATE INDEX idx_districts_tier ON districts(tier);
CREATE INDEX idx_districts_enrollment ON districts(enrollment);

-- ============================================================================
-- LEADS TABLE
-- Enriched contacts from Clay
-- ============================================================================
CREATE TABLE IF NOT EXISTS leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    district_id UUID REFERENCES districts(id),
    
    -- Person info
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),
    title VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    linkedin_url VARCHAR(500),
    
    -- Classification
    persona VARCHAR(50), -- 'superintendent', 'safety_director', 'coo'
    is_primary_contact BOOLEAN DEFAULT FALSE,
    lead_score INTEGER DEFAULT 0,
    
    -- Enrichment metadata
    enriched_at TIMESTAMP,
    enrichment_source VARCHAR(50), -- 'clay', 'manual', 'linkedin'
    clay_person_id VARCHAR(100),
    
    -- Campaign tracking
    campaign_id VARCHAR(100),
    instantly_lead_id VARCHAR(100),
    status VARCHAR(50) DEFAULT 'new', -- 'new', 'contacted', 'opened', 'replied', 'meeting_booked', 'closed'
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(email)
);

-- Indexes
CREATE INDEX idx_leads_district ON leads(district_id);
CREATE INDEX idx_leads_persona ON leads(persona);
CREATE INDEX idx_leads_status ON leads(status);
CREATE INDEX idx_leads_campaign ON leads(campaign_id);

-- ============================================================================
-- CAMPAIGNS TABLE
-- Email campaigns in Instantly.ai
-- ============================================================================
CREATE TABLE IF NOT EXISTS campaigns (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instantly_campaign_id VARCHAR(100) UNIQUE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    
    -- Targeting
    target_persona VARCHAR(50), -- 'superintendent', 'safety_director'
    target_tier VARCHAR(50),
    
    -- Status
    status VARCHAR(50) DEFAULT 'draft', -- 'draft', 'active', 'paused', 'completed'
    
    -- Stats (updated via webhook)
    total_leads INTEGER DEFAULT 0,
    emails_sent INTEGER DEFAULT 0,
    opens INTEGER DEFAULT 0,
    replies INTEGER DEFAULT 0,
    meetings_booked INTEGER DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- ============================================================================
-- EMAIL_SEQUENCES TABLE
-- Email templates and variations
-- ============================================================================
CREATE TABLE IF NOT EXISTS email_sequences (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id UUID REFERENCES campaigns(id),
    
    step_number INTEGER NOT NULL,
    delay_days INTEGER DEFAULT 0,
    
    -- Content (with spintax)
    subject_template TEXT,
    body_template TEXT,
    
    -- Variations generated
    variations_count INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- INTERACTIONS TABLE
-- All touchpoints with leads
-- ============================================================================
CREATE TABLE IF NOT EXISTS interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id UUID REFERENCES leads(id),
    campaign_id UUID REFERENCES campaigns(id),
    
    -- Interaction type
    type VARCHAR(50) NOT NULL, -- 'email_sent', 'email_opened', 'email_clicked', 'email_replied', 'meeting_booked', 'call', 'note'
    
    -- Details
    subject VARCHAR(500),
    content TEXT,
    sentiment VARCHAR(20), -- 'positive', 'neutral', 'negative', 'interested', 'not_interested'
    
    -- Instantly.ai tracking
    instantly_event_id VARCHAR(100),
    email_step INTEGER,
    
    -- Timestamps
    occurred_at TIMESTAMP DEFAULT NOW(),
    logged_at TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_interactions_lead ON interactions(lead_id);
CREATE INDEX idx_interactions_type ON interactions(type);
CREATE INDEX idx_interactions_occurred ON interactions(occurred_at);

-- ============================================================================
-- SLACK_ALERTS TABLE
-- Log of alerts sent to Slack
-- ============================================================================
CREATE TABLE IF NOT EXISTS slack_alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID REFERENCES interactions(id),
    
    channel VARCHAR(100),
    message_ts VARCHAR(50), -- Slack message timestamp
    alert_type VARCHAR(50), -- 'reply', 'meeting', 'hot_lead'
    
    sent_at TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- VIEWS FOR METABASE DASHBOARDS
-- ============================================================================

-- Campaign Performance Overview
CREATE OR REPLACE VIEW v_campaign_performance AS
SELECT 
    c.name as campaign_name,
    c.target_persona,
    c.status,
    c.total_leads,
    c.emails_sent,
    c.opens,
    c.replies,
    ROUND(100.0 * c.opens / NULLIF(c.emails_sent, 0), 1) as open_rate,
    ROUND(100.0 * c.replies / NULLIF(c.emails_sent, 0), 1) as reply_rate,
    c.meetings_booked,
    c.started_at
FROM campaigns c
ORDER BY c.started_at DESC;

-- Lead Status by Tier
CREATE OR REPLACE VIEW v_leads_by_tier AS
SELECT 
    d.tier,
    l.status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY d.tier), 1) as percentage
FROM leads l
JOIN districts d ON l.district_id = d.id
GROUP BY d.tier, l.status
ORDER BY d.tier, l.status;

-- Daily Activity
CREATE OR REPLACE VIEW v_daily_activity AS
SELECT 
    DATE(occurred_at) as date,
    type,
    COUNT(*) as count
FROM interactions
WHERE occurred_at > NOW() - INTERVAL '30 days'
GROUP BY DATE(occurred_at), type
ORDER BY date DESC, type;

-- Hot Leads (replied or meeting booked)
CREATE OR REPLACE VIEW v_hot_leads AS
SELECT 
    l.full_name,
    l.title,
    l.email,
    d.district_name,
    d.enrollment,
    l.status,
    MAX(i.occurred_at) as last_interaction
FROM leads l
JOIN districts d ON l.district_id = d.id
LEFT JOIN interactions i ON l.id = i.lead_id
WHERE l.status IN ('replied', 'meeting_booked')
GROUP BY l.id, l.full_name, l.title, l.email, d.district_name, d.enrollment, l.status
ORDER BY last_interaction DESC;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Update lead status based on interactions
CREATE OR REPLACE FUNCTION update_lead_status()
RETURNS TRIGGER AS $$
BEGIN
    -- Update lead status based on interaction type
    IF NEW.type = 'email_opened' AND (SELECT status FROM leads WHERE id = NEW.lead_id) = 'contacted' THEN
        UPDATE leads SET status = 'opened', updated_at = NOW() WHERE id = NEW.lead_id;
    ELSIF NEW.type = 'email_replied' THEN
        UPDATE leads SET status = 'replied', updated_at = NOW() WHERE id = NEW.lead_id;
    ELSIF NEW.type = 'meeting_booked' THEN
        UPDATE leads SET status = 'meeting_booked', updated_at = NOW() WHERE id = NEW.lead_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for status updates
CREATE TRIGGER trigger_update_lead_status
AFTER INSERT ON interactions
FOR EACH ROW
EXECUTE FUNCTION update_lead_status();

-- Update campaign stats
CREATE OR REPLACE FUNCTION update_campaign_stats()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE campaigns SET
        emails_sent = (SELECT COUNT(*) FROM interactions WHERE campaign_id = NEW.campaign_id AND type = 'email_sent'),
        opens = (SELECT COUNT(*) FROM interactions WHERE campaign_id = NEW.campaign_id AND type = 'email_opened'),
        replies = (SELECT COUNT(*) FROM interactions WHERE campaign_id = NEW.campaign_id AND type = 'email_replied'),
        meetings_booked = (SELECT COUNT(*) FROM interactions WHERE campaign_id = NEW.campaign_id AND type = 'meeting_booked')
    WHERE id = NEW.campaign_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for campaign stats
CREATE TRIGGER trigger_update_campaign_stats
AFTER INSERT ON interactions
FOR EACH ROW
EXECUTE FUNCTION update_campaign_stats();

-- ============================================================================
-- SEED DATA FOR DEMO
-- ============================================================================

-- Insert sample campaigns
INSERT INTO campaigns (instantly_campaign_id, name, target_persona, status, total_leads, started_at) VALUES
('camp_super_q1', 'TX Superintendents Q1 2025', 'superintendent', 'active', 98, NOW() - INTERVAL '7 days'),
('camp_safety_q1', 'TX Safety Directors Q1 2025', 'safety_director', 'active', 45, NOW() - INTERVAL '5 days')
ON CONFLICT DO NOTHING;

-- ============================================================================
-- PERMISSIONS (adjust as needed)
-- ============================================================================

-- Grant read access to Metabase user
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO metabase_user;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO metabase_user;

COMMENT ON TABLE districts IS 'Raw scraped Texas school district data';
COMMENT ON TABLE leads IS 'Enriched contacts from Clay with campaign tracking';
COMMENT ON TABLE campaigns IS 'Email campaigns synced with Instantly.ai';
COMMENT ON TABLE interactions IS 'All lead touchpoints and email events';
