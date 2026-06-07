-- ─────────────────────────────────────────
-- Ritel Community Screener — Supabase Schema v2
-- Run di Supabase SQL Editor
-- ─────────────────────────────────────────

-- COMPANIES TABLE (dari GoAPI IDX)
create table if not exists companies (
  id           bigserial primary key,
  symbol       varchar(10) not null unique,
  name         text,
  logo         text,
  sector       text,
  sub_sector   text,
  industry     text,
  listing_date date,
  created_at   timestamptz default now(),
  updated_at   timestamptz default now()
);
create index if not exists idx_companies_symbol on companies(symbol);
create index if not exists idx_companies_name on companies using gin(to_tsvector('simple', coalesce(name,'')));

-- API LOGS TABLE
create table if not exists api_logs (
  id           bigserial primary key,
  service_name varchar(100) not null,
  status       varchar(20) not null,  -- SUCCESS | ERROR | WARNING
  message      text,
  created_at   timestamptz default now()
);
create index if not exists idx_api_logs_service on api_logs(service_name);
create index if not exists idx_api_logs_created on api_logs(created_at desc);

-- STOCKS DATA TABLE (harga real-time via yfinance)
create table if not exists stocks_data (
  id         bigserial primary key,
  ticker     varchar(10) not null unique,
  price      numeric(15,2),
  volume     numeric(20,0),
  updated_at timestamptz default now()
);
create index if not exists idx_stocks_ticker on stocks_data(ticker);
create index if not exists idx_stocks_volume on stocks_data(volume desc);

-- USERS TABLE
create table if not exists users (
  id           bigserial primary key,
  phone_number varchar(20) unique,
  name         text,
  status       varchar(10) default 'Free',  -- Free | VIP
  created_at   timestamptz default now(),
  updated_at   timestamptz default now()
);

-- SCREENER ALERTS TABLE
create table if not exists screener_alerts (
  id                   bigserial primary key,
  ticker               varchar(10) not null,
  price                numeric(15,2),
  indicator_triggered  text,
  timestamp            timestamptz default now()
);
create index if not exists idx_alerts_timestamp on screener_alerts(timestamp desc);

-- Enable RLS (Row Level Security) — opsional
-- alter table companies enable row level security;
-- alter table api_logs enable row level security;
