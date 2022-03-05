/* contrib/aqo/aqo--1.3--1.4.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION aqo UPDATE TO '1.4'" to load this file. \quit

ALTER TABLE public.aqo_data ADD COLUMN reliability double precision [];
