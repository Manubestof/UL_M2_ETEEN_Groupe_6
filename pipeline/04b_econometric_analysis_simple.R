#!/usr/local/bin/Rscript
# ÉTAPE 4B : ANALYSE ÉCONOMÉTRIQUE SIMPLE
# 
# Version simplifiée avec spécification au niveau pays-année (agrégation préalable)

library(jsonlite)
library(dplyr)
library(readr)
library(fixest)

# === DÉTECTION ROBUSTE DU CHEMIN ET CHARGEMENT DE LA CONFIGURATION ===
get_script_dir <- function() {
  # Rscript: argv[1] est le chemin du script
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    return(dirname(sub("^--file=", "", file_arg[1])))
  }
  # RStudio
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    return(dirname(rstudioapi::getActiveDocumentContext()$path))
  }
  # Fallback: getwd()
  return(getwd())
}

script_dir <- get_script_dir()
project_root <- dirname(script_dir)
config_path <- file.path(script_dir, "config.json")
if (!file.exists(config_path)) config_path <- file.path(script_dir, "../pipeline/config.json")
config <- fromJSON(config_path)

# === FONCTION UTILITAIRE CORRIGÉE POUR FIXEST ===
make_table_df_correct <- function(vars, models, inter_vars = NULL, types = NULL, names = NULL) {
  n_vars <- length(vars)
  n_models <- length(models)
  
  result_df <- data.frame(
    Variable = names %||% vars,
    Type = types %||% rep("", n_vars),
    Main = character(n_vars),
    Interaction = character(n_vars),
    R2 = numeric(n_vars),
    Observations = integer(n_vars),
    FixedEffects = rep("Country, Year", n_vars),
    stringsAsFactors = FALSE
  )
  
  for (i in 1:n_vars) {
    model <- models[[i]]
    if (is.null(model) || inherits(model, "try-error")) {
      result_df$Main[i] <- "Model failed"
      result_df$Interaction[i] <- ""
      result_df$R2[i] <- NA
      result_df$Observations[i] <- 0
      next
    }
    
    # Pour les modèles fixest
    if (inherits(model, "fixest")) {
      # Obtenir les coefficients disponibles
      all_coefs <- coef(model)
      coef_names <- names(all_coefs)
      
      cat(sprintf("[DEBUG] Modèle %d (%s) - coefficients: %s\n", i, 
                  types[i], paste(coef_names, collapse = ", ")))
      
      # Extraire coefficient principal
      main_var <- vars[i]
      if (main_var %in% coef_names) {
        coef_val <- all_coefs[main_var]
        se_val <- se(model)[main_var]
        pval <- pvalue(model)[main_var]
        
        # Formatage avec significativité
        stars <- ""
        if (!is.na(pval)) {
          if (pval < 0.001) stars <- "***"
          else if (pval < 0.01) stars <- "**"
          else if (pval < 0.05) stars <- "*"
          else if (pval < 0.1) stars <- "°"
        }
        
        result_df$Main[i] <- sprintf("%.6f%s\n(%.6f)", coef_val, stars, se_val)
      } else {
        result_df$Main[i] <- sprintf("Variable '%s' not in model", main_var)
      }
      
      # Extraire coefficient d'interaction
      if (!is.null(inter_vars) && !is.na(inter_vars[i])) {
        inter_var <- inter_vars[i]
        if (inter_var %in% coef_names) {
          coef_val <- all_coefs[inter_var]
          se_val <- se(model)[inter_var]
          pval <- pvalue(model)[inter_var]
          
          stars <- ""
          if (!is.na(pval)) {
            if (pval < 0.001) stars <- "***"
            else if (pval < 0.01) stars <- "**"
            else if (pval < 0.05) stars <- "*"
            else if (pval < 0.1) stars <- "°"
          }
          
          result_df$Interaction[i] <- sprintf("%.6f%s\n(%.6f)", coef_val, stars, se_val)
        } else {
          result_df$Interaction[i] <- sprintf("Variable '%s' not in model", inter_var)
        }
      } else {
        result_df$Interaction[i] <- ""
      }
      
      # R² et observations
      result_df$R2[i] <- round(r2(model, type = "r2"), 4)
      result_df$Observations[i] <- model$nobs
    }
  }
  
  return(result_df)
}

# === CHARGEMENT DES DONNÉES ===
cat("=== ANALYSE ÉCONOMÉTRIQUE SIMPLE (TOUTES PÉRIODES) ===\n")

# Utilisation des chemins détectés
output_dir <- file.path(project_root, config$RESULTS_DIR, "tables_simple")

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# === TRAITEMENT DE TOUTES LES PÉRIODES ===
# Récupération des périodes depuis la configuration (même logique que 04_econometric_analysis.R)
EXPORT_PERIODS <- config$EXPORT_PERIODS
if (is.list(EXPORT_PERIODS) && is.numeric(EXPORT_PERIODS[[1]])) {
  # Cas normal : liste de vecteurs
  EXPORT_PERIODS <- lapply(EXPORT_PERIODS, function(x) as.integer(x))
} else if (is.matrix(EXPORT_PERIODS)) {
  # Cas matrice : conversion en liste
  EXPORT_PERIODS <- apply(EXPORT_PERIODS, 1, function(x) as.integer(x))
  EXPORT_PERIODS <- split(EXPORT_PERIODS, rep(1:nrow(EXPORT_PERIODS)))
} else if (is.vector(EXPORT_PERIODS) && length(EXPORT_PERIODS) %% 2 == 0) {
  # Cas vecteur plat : regroupement par paires
  EXPORT_PERIODS <- split(as.integer(EXPORT_PERIODS), ceiling(seq_along(EXPORT_PERIODS)/2))
}

cat(sprintf("Périodes configurées: %d\n", length(EXPORT_PERIODS)))

for (period in EXPORT_PERIODS) {
  start_year <- period[1]
  end_year <- period[2]
  period_str <- paste0(start_year, "_", end_year)
  
  cat(sprintf("\n[PÉRIODE] %d-%d\n", start_year, end_year))
  
  # Chemin du dataset pour cette période
  dataset_file <- file.path(project_root, config$DATASETS_DIR, 
                           paste0("econometric_dataset_", period_str, ".csv"))
  
  if (!file.exists(dataset_file)) {
    cat(sprintf("[SKIP] Dataset manquant: %s\n", dataset_file))
    next
  }
  
  data_raw <- read_csv(dataset_file, show_col_types = FALSE)
  
  # Préparation des données
  names(data_raw) <- tolower(names(data_raw))
  names(data_raw) <- gsub(" ", "_", names(data_raw))
  data_raw <- data_raw %>%
    rename(
      year = any_of(c("year", "Year")),
      country = any_of(c("country", "Country")),
      iso3 = any_of(c("iso", "ISO", "iso3"))
    )
  
  # Filtrage et agrégation au niveau pays-année
  data_with_events <- data_raw %>%
    filter(
      earthquake_sig_anydeaths == TRUE | 
      storm_sig_anydeaths == TRUE | 
      flood_sig_anydeaths == TRUE | 
      extreme_temperature_sig_anydeaths == TRUE
    )
  
  cat(sprintf("[FILTER] Observations avec événements significatifs: %d\n", nrow(data_with_events)))
  
  data_country_year <- data_with_events %>%
    group_by(iso3, country, year) %>%
    summarise(
      total_exports = sum(fobvalue, na.rm = TRUE),
      agri_exports = sum(ifelse(is_agri == TRUE, fobvalue, 0), na.rm = TRUE),
      ln_total_occurrence = first(ln_total_occurrence),
      ln_total_deaths = first(ln_total_deaths),
      disaster_index = first(disaster_index),
      ln_earthquake_count = first(ln_earthquake_count),
      ln_storm_count = first(ln_storm_count),
      ln_flood_count = first(ln_flood_count),
      ln_extreme_temperature_count = first(ln_extreme_temperature_count),
      is_poor_country = first(is_poor_country),
      is_small_country = first(is_small_country),
      d_ln_population = first(d_ln_population),
      .groups = "drop"
    ) %>%
    filter(
      total_exports > 0,
      !is.na(ln_total_occurrence),
      !is.na(is_poor_country),
      !is.na(is_small_country)
    ) %>%
    mutate(
      ln_total_exports = log(total_exports),
      ln_agri_exports = log(pmax(agri_exports, 1)),
      ln_total_occurrence_poor = ln_total_occurrence * is_poor_country,
      ln_total_deaths_poor = ln_total_deaths * is_poor_country,
      disaster_index_poor = disaster_index * is_poor_country,
      ln_total_occurrence_small = ln_total_occurrence * is_small_country,
      ln_total_deaths_small = ln_total_deaths * is_small_country,
      disaster_index_small = disaster_index * is_small_country,
      ln_earthquake_count_poor = ln_earthquake_count * is_poor_country,
      ln_storm_count_poor = ln_storm_count * is_poor_country,
      ln_flood_count_poor = ln_flood_count * is_poor_country,
      ln_extreme_temperature_count_poor = ln_extreme_temperature_count * is_poor_country,
      ln_earthquake_count_small = ln_earthquake_count * is_small_country,
      ln_storm_count_small = ln_storm_count * is_small_country,
      ln_flood_count_small = ln_flood_count * is_small_country,
      ln_extreme_temperature_count_small = ln_extreme_temperature_count * is_small_country
    )
  
  cat(sprintf("[AGGREGATION] Dataset final: %d observations pays-année\n", nrow(data_country_year)))
  
  # === MODÈLES TABLE 1 ===
  cat("\n[TABLE 1] Impact agrégé des catastrophes...\n")
  
  model1a <- try(feols(ln_total_exports ~ ln_total_occurrence + ln_total_occurrence_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model1b <- try(feols(ln_total_exports ~ ln_total_deaths + ln_total_deaths_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model1c <- try(feols(ln_total_exports ~ disaster_index + disaster_index_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model1d <- try(feols(ln_agri_exports ~ ln_total_occurrence + ln_total_occurrence_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model1e <- try(feols(ln_agri_exports ~ ln_total_deaths + ln_total_deaths_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model1f <- try(feols(ln_agri_exports ~ disaster_index + disaster_index_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  
  # Construction Table 1
  table1_vars <- c("ln_total_occurrence", "ln_total_deaths", "disaster_index",
                   "ln_total_occurrence", "ln_total_deaths", "disaster_index")
  table1_inter <- c("ln_total_occurrence_poor", "ln_total_deaths_poor", "disaster_index_poor",
                    "ln_total_occurrence_poor", "ln_total_deaths_poor", "disaster_index_poor")
  table1_types <- c("Total exports", "Total exports", "Total exports",
                    "Agricultural exports", "Agricultural exports", "Agricultural exports")
  table1_models <- list(model1a, model1b, model1c, model1d, model1e, model1f)
  
  table1_df <- make_table_df_correct(
    vars = table1_vars,
    models = table1_models,
    inter_vars = table1_inter,
    types = table1_types
  )
  
  # Sauvegarde Table 1
  table1_file <- file.path(output_dir, paste0("table1_simple_", period_str, ".csv"))
  write_csv(table1_df, table1_file)
  cat(sprintf("[SAVED] %s\n", table1_file))
  
  # === MODÈLES TABLE 2 (Types × Pauvres) ===
  cat("\n[TABLE 2] Types de catastrophes × pays pauvres...\n")
  
  model2a <- try(feols(ln_agri_exports ~ ln_flood_count + ln_flood_count_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model2b <- try(feols(ln_agri_exports ~ ln_storm_count + ln_storm_count_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model2c <- try(feols(ln_agri_exports ~ ln_earthquake_count + ln_earthquake_count_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model2d <- try(feols(ln_agri_exports ~ ln_extreme_temperature_count + ln_extreme_temperature_count_poor | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  
  table2_vars <- c("ln_flood_count", "ln_storm_count", "ln_earthquake_count", "ln_extreme_temperature_count")
  table2_inter <- c("ln_flood_count_poor", "ln_storm_count_poor", "ln_earthquake_count_poor", "ln_extreme_temperature_count_poor")
  table2_types <- c("Flood", "Storm", "Earthquake", "Extreme temperature")
  table2_models <- list(model2a, model2b, model2c, model2d)
  
  table2_df <- make_table_df_correct(
    vars = table2_vars,
    models = table2_models,
    inter_vars = table2_inter,
    types = table2_types
  )
  
  table2_file <- file.path(output_dir, paste0("table2_simple_", period_str, ".csv"))
  write_csv(table2_df, table2_file)
  cat(sprintf("[SAVED] %s\n", table2_file))
  
  # === MODÈLES TABLE 3 (Types × Petits) ===
  cat("\n[TABLE 3] Types de catastrophes × pays petits...\n")
  
  model3a <- try(feols(ln_agri_exports ~ ln_flood_count + ln_flood_count_small | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model3b <- try(feols(ln_agri_exports ~ ln_storm_count + ln_storm_count_small | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model3c <- try(feols(ln_agri_exports ~ ln_earthquake_count + ln_earthquake_count_small | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  model3d <- try(feols(ln_agri_exports ~ ln_extreme_temperature_count + ln_extreme_temperature_count_small | iso3 + year, 
                       data = data_country_year, cluster = "iso3"), silent = TRUE)
  
  table3_vars <- c("ln_flood_count", "ln_storm_count", "ln_earthquake_count", "ln_extreme_temperature_count")
  table3_inter <- c("ln_flood_count_small", "ln_storm_count_small", "ln_earthquake_count_small", "ln_extreme_temperature_count_small")
  table3_types <- c("Flood", "Storm", "Earthquake", "Extreme temperature")
  table3_models <- list(model3a, model3b, model3c, model3d)
  
  table3_df <- make_table_df_correct(
    vars = table3_vars,
    models = table3_models,
    inter_vars = table3_inter,
    types = table3_types
  )
  
  table3_file <- file.path(output_dir, paste0("table3_simple_", period_str, ".csv"))
  write_csv(table3_df, table3_file)
  cat(sprintf("[SAVED] %s\n", table3_file))
}

cat("✅ Analyse terminée. Tables sauvegardées dans:", output_dir, "\n")
