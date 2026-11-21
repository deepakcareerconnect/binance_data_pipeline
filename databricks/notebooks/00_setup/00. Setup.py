# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS binance_data;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS binance_data.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS binance_data.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS binance_data.gold_analytics;
# MAGIC CREATE SCHEMA IF NOT EXISTS binance_data.gold_ai;
# MAGIC CREATE SCHEMA IF NOT EXISTS binance_data.monitoring;