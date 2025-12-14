-- ============================================================================
-- METABASE DASHBOARD QUERIES
-- Ambient.ai GTM Campaign Analytics
-- ============================================================================

-- ============================================================================
-- DASHBOARD 1: CAMPAIGN OVERVIEW
-- ============================================================================

-- Card 1: Total Leads Contacted
SELECT COUNT(*) as total_leads
FROM leads
WHERE status != 'new';

-- Card 2: Open Rate
SELECT 
    ROUND(100.0 * COUNT(CASE WHEN status IN ('opened', 'replied', 'meeting_booked') THEN 1 END) 
          / NULLIF(COUNT(*), 0), 1) as open_rate_pct
FROM leads
WHERE status != 'new';

-- Card 3: Reply Rate
SELECT 
    ROUND(100.0 * COUNT(CASE WHEN status IN ('replied', 'meeting_booked') THEN 1 END) 
          / NULLIF(COUNT(*), 0), 1) as reply_rate_pct
FROM leads
WHERE status != 'new';

-- Card 4: Meetings Booked
SELECT COUNT(*) as meetings
FROM leads
WHERE status = 'meeting_booked';

-- ============================================================================
-- DASHBOARD 2: CAMPAIGN COMPARISON
-- ============================================================================

-- Campaign Performance Side-by-Side
SELECT 
    c.name as campaign,
    c.target_persona as persona,
    c.total_leads,
    c.emails_sent,
    c.opens,
    c.replies,
    ROUND(100.0 * c.opens / NULLIF(c.emails_sent, 0), 1) as open_rate,
    ROUND(100.0 * c.replies / NULLIF(c.emails_sent, 0), 1) as reply_rate,
    c.meetings_booked
FROM campaigns c
WHERE c.status = 'active'
ORDER BY c.reply_rate DESC;

-- ============================================================================
-- DASHBOARD 3: LEAD FUNNEL
-- ============================================================================

-- Funnel by Status
SELECT 
    status,
    COUNT(*) as count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as percentage
FROM leads
GROUP BY status
ORDER BY 
    CASE status
        WHEN 'new' THEN 1
        WHEN 'contacted' THEN 2
        WHEN 'opened' THEN 3
        WHEN 'replied' THEN 4
        WHEN 'meeting_booked' THEN 5
        WHEN 'closed' THEN 6
    END;

-- ============================================================================
-- DASHBOARD 4: PERFORMANCE BY TIER
-- ============================================================================

-- Reply Rate by District Tier
SELECT 
    d.tier,
    COUNT(DISTINCT l.id) as total_leads,
    COUNT(CASE WHEN l.status IN ('replied', 'meeting_booked') THEN 1 END) as replies,
    ROUND(100.0 * COUNT(CASE WHEN l.status IN ('replied', 'meeting_booked') THEN 1 END) 
          / NULLIF(COUNT(DISTINCT l.id), 0), 1) as reply_rate
FROM leads l
JOIN districts d ON l.district_id = d.id
WHERE l.status != 'new'
GROUP BY d.tier
ORDER BY reply_rate DESC;

-- ============================================================================
-- DASHBOARD 5: SUBJECT LINE A/B TEST
-- ============================================================================

-- Subject Line Performance (requires tracking in email_sends table)
SELECT 
    subject_line,
    COUNT(*) as sent,
    COUNT(CASE WHEN opened THEN 1 END) as opens,
    COUNT(CASE WHEN replied THEN 1 END) as replies,
    ROUND(100.0 * COUNT(CASE WHEN opened THEN 1 END) / NULLIF(COUNT(*), 0), 1) as open_rate,
    ROUND(100.0 * COUNT(CASE WHEN replied THEN 1 END) / NULLIF(COUNT(*), 0), 1) as reply_rate
FROM email_sends
WHERE sent_at > NOW() - INTERVAL '14 days'
GROUP BY subject_line
HAVING COUNT(*) >= 20  -- Minimum sample size
ORDER BY reply_rate DESC
LIMIT 10;

-- ============================================================================
-- DASHBOARD 6: DAILY ACTIVITY TREND
-- ============================================================================

-- Daily Sends, Opens, Replies (Last 30 Days)
SELECT 
    DATE(occurred_at) as date,
    COUNT(CASE WHEN type = 'email_sent' THEN 1 END) as sent,
    COUNT(CASE WHEN type = 'email_opened' THEN 1 END) as opened,
    COUNT(CASE WHEN type = 'email_replied' THEN 1 END) as replied
FROM interactions
WHERE occurred_at > NOW() - INTERVAL '30 days'
GROUP BY DATE(occurred_at)
ORDER BY date;

-- ============================================================================
-- DASHBOARD 7: HOT LEADS
-- ============================================================================

-- Recent Replies (Action Required)
SELECT 
    l.full_name,
    l.title,
    l.email,
    d.district_name,
    d.enrollment,
    i.content as reply_preview,
    i.sentiment,
    i.occurred_at
FROM leads l
JOIN districts d ON l.district_id = d.id
JOIN interactions i ON l.id = i.lead_id
WHERE i.type = 'email_replied'
  AND i.occurred_at > NOW() - INTERVAL '7 days'
ORDER BY i.occurred_at DESC
LIMIT 20;

-- ============================================================================
-- DASHBOARD 8: ENROLLMENT ANALYSIS
-- ============================================================================

-- Performance by Enrollment Size
SELECT 
    CASE 
        WHEN d.enrollment >= 50000 THEN '50K+ (Enterprise)'
        WHEN d.enrollment >= 20000 THEN '20K-50K (Large)'
        WHEN d.enrollment >= 10000 THEN '10K-20K (Mid)'
        WHEN d.enrollment >= 5000 THEN '5K-10K (Small)'
        ELSE '<5K (Tiny)'
    END as size_bucket,
    COUNT(DISTINCT l.id) as leads,
    COUNT(CASE WHEN l.status IN ('replied', 'meeting_booked') THEN 1 END) as responses,
    ROUND(100.0 * COUNT(CASE WHEN l.status IN ('replied', 'meeting_booked') THEN 1 END) 
          / NULLIF(COUNT(DISTINCT l.id), 0), 1) as response_rate
FROM leads l
JOIN districts d ON l.district_id = d.id
WHERE l.status != 'new'
GROUP BY size_bucket
ORDER BY 
    CASE size_bucket
        WHEN '50K+ (Enterprise)' THEN 1
        WHEN '20K-50K (Large)' THEN 2
        WHEN '10K-20K (Mid)' THEN 3
        WHEN '5K-10K (Small)' THEN 4
        ELSE 5
    END;

-- ============================================================================
-- DASHBOARD 9: PERSONA COMPARISON
-- ============================================================================

-- Superintendent vs Safety Director Performance
SELECT 
    l.persona,
    COUNT(*) as total_leads,
    COUNT(CASE WHEN l.status = 'opened' THEN 1 END) as opened,
    COUNT(CASE WHEN l.status = 'replied' THEN 1 END) as replied,
    COUNT(CASE WHEN l.status = 'meeting_booked' THEN 1 END) as meetings,
    ROUND(100.0 * COUNT(CASE WHEN l.status = 'replied' THEN 1 END) 
          / NULLIF(COUNT(*), 0), 1) as reply_rate
FROM leads l
WHERE l.status != 'new'
GROUP BY l.persona;

-- ============================================================================
-- DASHBOARD 10: WEEKLY SUMMARY
-- ============================================================================

-- This Week vs Last Week
WITH this_week AS (
    SELECT 
        COUNT(CASE WHEN type = 'email_sent' THEN 1 END) as sent,
        COUNT(CASE WHEN type = 'email_opened' THEN 1 END) as opened,
        COUNT(CASE WHEN type = 'email_replied' THEN 1 END) as replied,
        COUNT(CASE WHEN type = 'meeting_booked' THEN 1 END) as meetings
    FROM interactions
    WHERE occurred_at > DATE_TRUNC('week', NOW())
),
last_week AS (
    SELECT 
        COUNT(CASE WHEN type = 'email_sent' THEN 1 END) as sent,
        COUNT(CASE WHEN type = 'email_opened' THEN 1 END) as opened,
        COUNT(CASE WHEN type = 'email_replied' THEN 1 END) as replied,
        COUNT(CASE WHEN type = 'meeting_booked' THEN 1 END) as meetings
    FROM interactions
    WHERE occurred_at BETWEEN DATE_TRUNC('week', NOW()) - INTERVAL '7 days' 
                          AND DATE_TRUNC('week', NOW())
)
SELECT 
    'This Week' as period,
    tw.sent, tw.opened, tw.replied, tw.meetings
FROM this_week tw
UNION ALL
SELECT 
    'Last Week' as period,
    lw.sent, lw.opened, lw.replied, lw.meetings
FROM last_week lw;

-- ============================================================================
-- ALERTS: Queries for n8n/Slack Notifications
-- ============================================================================

-- Hot Leads (Positive Replies in Last Hour)
SELECT 
    l.full_name,
    l.email,
    d.district_name,
    i.content as reply,
    i.sentiment
FROM leads l
JOIN districts d ON l.district_id = d.id
JOIN interactions i ON l.id = i.lead_id
WHERE i.type = 'email_replied'
  AND i.sentiment = 'positive'
  AND i.occurred_at > NOW() - INTERVAL '1 hour';

-- Stale Leads (Opened but No Reply in 3+ Days)
SELECT 
    l.full_name,
    l.email,
    d.district_name,
    MAX(i.occurred_at) as last_open
FROM leads l
JOIN districts d ON l.district_id = d.id
JOIN interactions i ON l.id = i.lead_id
WHERE l.status = 'opened'
  AND i.type = 'email_opened'
GROUP BY l.id, l.full_name, l.email, d.district_name
HAVING MAX(i.occurred_at) < NOW() - INTERVAL '3 days';
