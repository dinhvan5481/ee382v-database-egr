CREATE DATABASE "ee382v-db-engr"
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

GRANT TEMPORARY, CONNECT ON DATABASE "ee382v-db-engr" TO PUBLIC;

GRANT ALL ON DATABASE "ee382v-db-engr" TO postgres;

GRANT TEMPORARY ON DATABASE "ee382v-db-engr" TO python;
