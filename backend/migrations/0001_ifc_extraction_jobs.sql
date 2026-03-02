create extension if not exists pgcrypto;

create table if not exists ifc_extraction_jobs (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  status text not null check (status in ('queued','running','done','failed','canceled')),
  progress int not null default 0 check (progress >= 0 and progress <= 100),
  message text,
  requested_by text,
  input_files jsonb not null,
  options jsonb not null,
  result jsonb,
  error text,
  locked_at timestamptz,
  locked_by text,
  attempts int not null default 0
);

create index if not exists ix_ifc_extraction_jobs_status_created_at on ifc_extraction_jobs(status, created_at);
create index if not exists ix_ifc_extraction_jobs_locked_at on ifc_extraction_jobs(locked_at);

create or replace function set_ifc_job_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_ifc_jobs_updated_at on ifc_extraction_jobs;
create trigger trg_ifc_jobs_updated_at
before update on ifc_extraction_jobs
for each row execute function set_ifc_job_updated_at();
