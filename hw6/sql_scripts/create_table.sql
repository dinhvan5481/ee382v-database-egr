CREATE TABLE public.benchmark
(
    "theKey" bigint NOT NULL,
    "columnA" integer,
    "columnB" integer,
    filter character(247),
    CONSTRAINT benchmark_pkey PRIMARY KEY ("theKey")
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.benchmark
    OWNER to python;
