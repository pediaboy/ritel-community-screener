-- ────────────────────────────────────────────
-- RITEL COMMUNITY SCREENER — Database Schema
-- Run this in your Supabase SQL Editor
-- ────────────────────────────────────────────

-- 1. USERS TABLE
CREATE TABLE IF NOT EXISTS public.users (
  id          uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  phone_number text UNIQUE NOT NULL,
  name        text,
  status      text DEFAULT 'Free' CHECK (status IN ('Free', 'VIP')),
  created_at  timestamp WITH TIME ZONE DEFAULT now()
);

-- 2. STOCKS_DATA TABLE
CREATE TABLE IF NOT EXISTS public.stocks_data (
  ticker      text PRIMARY KEY,
  price       numeric,
  volume      numeric,
  updated_at  timestamp WITH TIME ZONE DEFAULT now()
);

-- 3. SCREENER_ALERTS TABLE
CREATE TABLE IF NOT EXISTS public.screener_alerts (
  id                 serial PRIMARY KEY,
  ticker             text NOT NULL,
  price              numeric,
  indicator_triggered text,
  timestamp          timestamp WITH TIME ZONE DEFAULT now()
);

-- Enable Row Level Security (optional — disable for admin service role)
ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.stocks_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.screener_alerts ENABLE ROW LEVEL SECURITY;

-- Allow service_role full access (bypass RLS)
CREATE POLICY "service_role_all" ON public.users FOR ALL USING (true);
CREATE POLICY "service_role_all" ON public.stocks_data FOR ALL USING (true);
CREATE POLICY "service_role_all" ON public.screener_alerts FOR ALL USING (true);

-- Grant anon read on stocks & alerts (public data)
CREATE POLICY "anon_read_stocks" ON public.stocks_data FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read_alerts" ON public.screener_alerts FOR SELECT TO anon USING (true);

-- Sample data
INSERT INTO public.stocks_data (ticker, price, volume) VALUES
  ('BBRI', 4820, 210000000),
  ('AMMN', 8750, 145000000),
  ('TLKM', 3740, 98000000),
  ('ASII', 5200, 76000000),
  ('BMRI', 6100, 132000000)
ON CONFLICT (ticker) DO NOTHING;

