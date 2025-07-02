#!/usr/local/bin/Rscript
# ÉTAPE 4 : ANALYSE ÉCONOMÉTRIQUE COMPLÈTE
# 
# Analyse de l'impact des catastrophes naturelles sur les exportations
# "Natural disasters and countries exports: New insights from a new (and an old) database"
#
# GÉNÉRATION DE TOUTES LES TABLES (1-7 + A1-A2) :
# Table 1: Disasters, exports and poor countries (all four types pooled)
# Table 2: Types of disasters, agricultural exports and poor countries  
# Table 3: Types of disasters, agricultural exports and small countries
# Table 4: Comparison EM-DAT vs GeoMet (synthesis)
# Table 5: Extreme disasters (top 10% human damage, EM-DAT)
# Table 6: Extreme disasters (top 10% physical damage, GeoMet)  
# Table 7: Comparison extreme disasters (synthesis)
# Table A1: Extreme disasters (top 20% human damage) - Appendix
# Table A2: Extreme disasters (top 20% physical damage) - Appendix

# Configuration dynamique : lecture de config.json pour piloter toute l'analyse
library(jsonlite)
library(dplyr)
library(readr)
library(tidyr)

# === SÉLECTION DES MÉTRIQUES PRINCIPALES POUR CHAQUE TABLE ===
# Modifiez ici pour choisir les variables utilisées dans les modèles principaux
# ---- Table 1 ----
# Par défaut : on considère uniquement les événements significatifs, comme dans l'article
# On filtre data_all et data_agri pour ne garder que les observations avec au moins un flag *_sig_* à TRUE
# (on suppose que les colonnes *_sig_* sont des indicateurs booléens d'événement significatif)
# (NOTE: Le filtrage est désormais effectué APRÈS la création de data_all/data_agri dans la boucle principale)
# Sélection des métriques principales (événements significatifs uniquement)
metric_table1_all <- c("ln_total_occurrence", "ln_total_deaths", "disaster_index")
metric_table1_agri <- c("ln_total_occurrence", "ln_total_deaths", "disaster_index")
# Pour tester d'autres métriques, modifiez simplement ces vecteurs !
# Exemples :
# metric_table1_all <- c("ln_total_occurrence", "ln_total_deaths")
# metric_table1_agri <- c("ln_total_occurrence", "disaster_index")
# ---- Table 2/3 (en commentaire, à adapter plus bas) ----
# table2_vars <- c("ln_flood_count", "ln_storm_count", "ln_earthquake_count", "ln_extreme_temperature_count")
# table2_inter <- c("ln_flood_count:is_poor_country", ...)
# table3_vars <- c("ln_flood_count", ...)
# table3_inter <- c("ln_flood_count:is_small_country", ...)
# =====================================

# === Robust helper functions (define ONCE, at the top) ===
save_table_csv <- function(df, filename) {
  write.csv(df, file = file.path(TABLES_DIR, filename), row.names = FALSE)
}

# Fonction pour formater les coefficients selon les standards académiques
format_coeff <- function(coef, se, pval) {
  if (any(is.na(c(coef, se, pval)))) return("")
  
  # Étoiles de significativité
  stars <- ""
  if (pval < 0.001) stars <- "***"
  else if (pval < 0.01) stars <- "**"
  else if (pval < 0.05) stars <- "*"
  else if (pval < 0.1) stars <- "°"
  
  # Format: coefficient(erreur-type)étoiles
  sprintf("%.4f%s\n(%.4f)", coef, stars, se)
}

make_table_df <- function(vars, models, inter_vars = NULL, types = NULL, names = NULL, fixed_effects = "Year, Country×Product, Product×Year") {
  keep <- which(!sapply(models, is.null))
  models <- models[keep]
  n <- length(models)
  
  # S'assurer que tous les vecteurs ont la bonne longueur
  if (!is.null(vars)) vars <- vars[keep]
  if (!is.null(types)) types <- types[keep]
  if (!is.null(names)) names <- names[keep]
  if (!is.null(inter_vars)) inter_vars <- inter_vars[keep]
  
  safe_extract <- function(i, fun, varname) {
    if (i > length(models) || is.null(models[[i]]) || is.null(varname) || is.na(varname) || !nzchar(varname)) return(c(NA, NA, NA))
    fun(models[[i]], varname)
  }
  
  # Extraire et formater les coefficients principaux
  main_coeffs <- sapply(seq_len(n), function(i) {
    v <- if (!is.null(vars) && length(vars) >= i) vars[i] else NA
    if (is.null(v) || is.na(v) || !nzchar(v)) return("")
    stats <- safe_extract(i, extract_stats, v)
    if (any(is.na(stats))) return("")
    format_coeff(stats[1], stats[2], stats[3])
  })
  
  # Extraire et formater les interactions
  inter_coeffs <- if (!is.null(inter_vars)) {
    sapply(seq_len(n), function(i) {
      stats <- safe_extract(i, extract_stats, inter_vars[i])
      if (any(is.na(stats))) return("")
      format_coeff(stats[1], stats[2], stats[3])
    })
  } else rep("", n)
  
  # Extraire R² et observations
  r2 <- sapply(models, function(m) if (!is.null(m)) round(summary(m)$r.squared, 4) else NA)
  obs <- sapply(models, function(m) if (!is.null(m)) nobs(m) else NA)
  
  df <- data.frame(
    Variable = if (!is.null(names)) names else vars,
    Type = if (!is.null(types)) types else rep("", n),
    Main = main_coeffs,
    Interaction = inter_coeffs,
    R2 = r2,
    Observations = obs,
    FixedEffects = rep(fixed_effects, n),
    stringsAsFactors = FALSE
  )
  return(df)
}
# Détection robuste du chemin du script ou fallback sur getwd()
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

# Extraction dynamique des paramètres
DATA_DIR <- file.path(project_root, config$DATA_DIR)
CACHE_DIR <- file.path(project_root, config$CACHE_DIR)
DATASETS_DIR <- file.path(project_root, config$DATASETS_DIR)
RESULTS_DIR <- file.path(project_root, config$RESULTS_DIR %||% config$DATASETS_DIR)
TABLES_DIR <- file.path(project_root, config$TABLES_DIR %||% file.path(config$RESULTS_DIR, "tables"))
EXPORT_PERIODS <- lapply(config$EXPORT_PERIODS, function(x) as.integer(x))
DISASTER_TYPES <- config$DISASTER_TYPES

# Correction du parsing des périodes : chaque élément doit être un vecteur de 2 entiers
EXPORT_PERIODS <- config$EXPORT_PERIODS
if (is.list(EXPORT_PERIODS) && is.numeric(EXPORT_PERIODS[[1]])) {
  # Cas normal : liste de vecteurs
  EXPORT_PERIODS <- lapply(EXPORT_PERIODS, function(x) as.integer(x))
} else if (is.matrix(EXPORT_PERIODS)) {
  # Cas rare : matrix
  EXPORT_PERIODS <- split(EXPORT_PERIODS, rep(1:ncol(EXPORT_PERIODS), each=nrow(EXPORT_PERIODS)))
  EXPORT_PERIODS <- lapply(EXPORT_PERIODS, as.integer)
} else if (is.vector(EXPORT_PERIODS) && length(EXPORT_PERIODS) %% 2 == 0) {
  # Cas plat : vector
  EXPORT_PERIODS <- split(as.integer(EXPORT_PERIODS), ceiling(seq_along(EXPORT_PERIODS)/2))
}

# Création des dossiers résultats et tables selon config.json
# (Suppression: la step 4 ne doit jamais créer de dossiers, elle ne fait que lire)
# if (!dir.exists(RESULTS_DIR)) dir.create(RESULTS_DIR, recursive = TRUE)
# if (!dir.exists(TABLES_DIR)) dir.create(TABLES_DIR, recursive = TRUE)

# Affichage récapitulatif des chemins utilisés
cat("\n[CONFIG] Chemins utilisés :\n")
cat("   • script_dir:", script_dir, "\n")
cat("   • project_root:", project_root, "\n")
cat("   • DATA_DIR:", DATA_DIR, "\n")
cat("   • CACHE_DIR:", CACHE_DIR, "\n")
cat("   • DATASETS_DIR:", DATASETS_DIR, "\n")
cat("   • RESULTS_DIR:", RESULTS_DIR, "\n")
cat("   • TABLES_DIR:", TABLES_DIR, "\n\n")
cat("[LOGGER] Début de la boucle sur les périodes définies dans config.json\n")
cat("DEBUG: début du for EXPORT_PERIODS\n")

n_found <- 0
for (period in EXPORT_PERIODS) {
  cat("\n==============================\n")
  cat(paste0("[PERIODE] Début : ", paste(period, collapse = "-"), "\n"))
  cat("==============================\n")
  if (length(period) != 2 || any(is.na(period))) {
    cat("[LOGGER] Période mal formée, on passe.\n")
    next  # Ignore les périodes mal formées
  }
  year_start <- period[1]
  year_end <- period[2]
  period_str <- paste0(year_start, "_", year_end)
  dataset_path <- file.path(DATASETS_DIR, paste0("econometric_dataset_", period_str, ".csv"))
  cat(paste0("[LOGGER] Chemin dataset : ", dataset_path, "\n"))
  if (!file.exists(dataset_path)) {
    cat(paste0("[LOGGER] ❌ Dataset manquant pour la période: ", dataset_path, "\n"))
    stop(paste("❌ Dataset manquant pour la période:", dataset_path))
  }
  n_found <- n_found + 1
  cat("[LOGGER] Dataset trouvé, chargement...\n")
  data <- read.csv(dataset_path, stringsAsFactors = FALSE)
  # Renommage automatique des colonnes si besoin
  col_rename_map <- c(
    "ISO" = "iso3",
    "Year" = "year",
    "cmdCode" = "product_code",
    "Income group" = "income_group",
    "Population" = "population"
  )
  for (old in names(col_rename_map)) {
    if (old %in% names(data)) names(data)[names(data) == old] <- col_rename_map[[old]]
  }
  # Créer d_ln_export_value si absent
  if (!"d_ln_export_value" %in% names(data)) {
    data <- data[order(data$iso3, data$product_code, data$year), ]
    export_cols <- c("export_value", "fobvalue", "ExportValue", "value")
    export_col <- export_cols[export_cols %in% names(data)][1]
    if (is.na(export_col)) {
      stop("❌ Aucune colonne d'export trouvée (essayé : export_value, fobvalue, ExportValue, value)")
    }
    data$ln_export_value <- log(data[[export_col]] + 1)
    data$d_ln_export_value <- ave(data$ln_export_value, data$iso3, data$product_code, FUN = function(x) c(NA, diff(x)))
  }
  # --- Création des variables nécessaires à l'analyse ---
  # 1. ln_total_occurrence : log(1 + somme des événements majeurs toutes catastrophes)
  event_cols <- grep("_events$", names(data), value = TRUE)
  if (length(event_cols) > 0) {
    data$sum_events <- rowSums(data[, event_cols, drop=FALSE], na.rm=TRUE)
    data$ln_total_occurrence <- log1p(data$sum_events)
  }
  # 2. ln_total_deaths : log(1 + somme des morts toutes catastrophes)
  death_cols <- grep("_deaths$", names(data), value = TRUE)
  if (length(death_cols) > 0) {
    data$sum_deaths <- rowSums(data[, death_cols, drop=FALSE], na.rm=TRUE)
    data$ln_total_deaths <- log1p(data$sum_deaths)
  }
  # 3. disaster_index : somme pondérée des intensités normalisées (GeoMet)
  intensity_cols <- grep("_intensity$", names(data), value = TRUE)
  if (length(intensity_cols) > 0) {
    # Normalisation par écart-type
    norm_intensities <- lapply(intensity_cols, function(col) {
      sd_col <- sd(data[[col]], na.rm=TRUE)
      if (sd_col > 0) data[[col]] / sd_col else rep(0, nrow(data))
    })
    data$disaster_index <- Reduce(`+`, norm_intensities)
  }
  # 4. d_ln_population : différence du log de la population par pays-année
  if ("population" %in% names(data)) {
    data <- data[order(data$iso3, data$product_code, data$year), ]
    data$ln_population <- log(data$population)
    data$d_ln_population <- ave(data$ln_population, data$iso3, data$product_code, FUN=function(x) c(NA, diff(x)))
  }
  
  # === CRÉATION DES VARIABLES EXTRÊMES POUR TABLES 5 ET 6 ===
  # 5. Variables d'occurrences par type de catastrophe (ln_*_occurrence)
  disaster_types_vars <- c("earthquake", "storm", "flood", "extreme_temperature")
  for (dtype in disaster_types_vars) {
    event_col <- paste0(dtype, "_events")
    ln_var <- paste0("ln_", dtype, "_occurrence")
    if (event_col %in% names(data)) {
      data[[ln_var]] <- log1p(data[[event_col]])
    }
  }
  
  # 5b. Variables de mortalité par type de catastrophe (ln_*_deaths)
  for (dtype in disaster_types_vars) {
    death_col <- paste0(dtype, "_deaths")
    ln_var <- paste0("ln_", dtype, "_deaths")
    if (death_col %in% names(data)) {
      data[[ln_var]] <- log1p(data[[death_col]])
    }
  }
  
  # 6. Variables extrêmes EM-DAT (top 10% par nombre de morts)
  for (dtype in disaster_types_vars) {
    death_col <- paste0(dtype, "_deaths")
    extreme_var <- paste0("extreme_", dtype, "_emdat")
    if (death_col %in% names(data)) {
      # Calculer le seuil top 10% des morts pour ce type de catastrophe
      threshold_90p <- quantile(data[[death_col]], 0.9, na.rm=TRUE)
      data[[extreme_var]] <- ifelse(data[[death_col]] >= threshold_90p & data[[death_col]] > 0, 1, 0)
      data[[extreme_var]][is.na(data[[death_col]])] <- 0
    }
  }
  
  # 7. Variables extrêmes GeoMet (top 10% par intensité physique)
  for (dtype in disaster_types_vars) {
    intensity_col <- paste0(dtype, "_intensity")
    extreme_var <- paste0("extreme_", dtype, "_geomet")
    if (intensity_col %in% names(data)) {
      # Calculer le seuil top 10% de l'intensité pour ce type de catastrophe
      threshold_90p <- quantile(data[[intensity_col]], 0.9, na.rm=TRUE)
      data[[extreme_var]] <- ifelse(data[[intensity_col]] >= threshold_90p & data[[intensity_col]] > 0, 1, 0)
      data[[extreme_var]][is.na(data[[intensity_col]])] <- 0
    }
  }
  # --- Correction : création explicite de data_all et data_agri ---
  if ("is_agri" %in% names(data)) {
    # Correction : conversion robuste de is_agri en booléen natif (AVANT data_all <- data)
    agri_true <- c("true", "1", "t", "yes", "y", "oui", "vrai", TRUE, 1)
    agri_false <- c("false", "0", "f", "no", "n", "non", "faux", FALSE, 0)
    data$is_agri <- tolower(trimws(as.character(data$is_agri)))
    data$is_agri[data$is_agri %in% agri_true] <- TRUE
    data$is_agri[data$is_agri %in% agri_false] <- FALSE
    data$is_agri <- as.logical(data$is_agri)
    data_all <- data
    data_agri <- subset(data, is_agri)
  } else {
    stop("❌ Colonne is_agri absente du dataset, impossible de créer data_agri.")
  }
  # --- Filtrage événements significatifs pour Table 1
  sig_cols <- grep('_sig_', names(data_all), value=TRUE)
  if (length(sig_cols) > 0) {
    data_all_sig <- data_all[rowSums(data_all[, sig_cols], na.rm=TRUE) > 0, ]
    data_agri_sig <- data_agri[rowSums(data_agri[, sig_cols], na.rm=TRUE) > 0, ]
  } else {
    stop("❌ Aucune colonne *_sig_* trouvée pour filtrer les événements significatifs.")
  }
  # --- FIN CONFIGURATION DYNAMIQUE ---

  # --- DIAGNOSTIC DEBUG ---
  cat("\n[DATA] Dataset chargé pour la période : ", period_str, "\n")
  cat("[DATA] Colonnes : ", paste(names(data), collapse=", "), "\n")
  cat("[DATA] Dimensions : ", dim(data)[1], "lignes, ", dim(data)[2], "colonnes\n")
  cat("[DATA] Niveaux de is_agri : ", if ("is_agri" %in% names(data)) paste(unique(data$is_agri), collapse=", ") else "(absent)", "\n")
  cat("[DATA] Taille de data_all : ", nrow(data_all), "\n")
  cat("[DATA] Taille de data_agri : ", if (exists("data_agri")) nrow(data_agri) else "(absent)", "\n")
  if (exists("data_agri") && nrow(data_agri) == 0) {
    stop("❌ data_agri est vide : aucun enregistrement avec is_agri == 1. Vérifiez la colonne is_agri dans le dataset.")
  }
  # Afficher les variables nécessaires pour Table 1
  required_vars <- c("ln_total_occurrence", "ln_total_deaths", "disaster_index", "d_ln_export_value", "is_poor_country", "d_ln_population", "iso3", "product_code", "year")
  cat("\n[CHECK] Variables nécessaires pour Table 1 :\n")
  for (v in required_vars) {
    cat("   -", v, if (v %in% names(data_all)) "[OK]" else "[ABSENT]", "\n")
  }
  cat("[CHECK] Variables nécessaires dans data_agri :\n")
  for (v in required_vars) {
    cat("   -", v, if (v %in% names(data_agri)) "[OK]" else "[ABSENT]", "\n")
  }
  # --- FIN DIAGNOSTIC ---

  # --- DIAGNOSTIC DEBUG AVANCÉ : distribution flags et is_agri ---
  sig_cols <- grep('_sig_', names(data), value=TRUE)
  cat("\n[DEBUG] Distribution des *_sig_* dans data_all :\n")
  print(colSums(data_all[, sig_cols, drop=FALSE], na.rm=TRUE))
  cat("[DEBUG] N lignes data_all :", nrow(data_all), "\n")
  cat("[DEBUG] N lignes is_agri==TRUE :", sum(data_all$is_agri, na.rm=TRUE), "\n")
  cat("[DEBUG] N lignes avec au moins un flag *_sig_* :", sum(rowSums(data_all[, sig_cols], na.rm=TRUE) > 0), "\n")
  cat("[DEBUG] N lignes is_agri==TRUE & au moins un flag *_sig_* :", sum(data_all$is_agri & rowSums(data_all[, sig_cols], na.rm=TRUE) > 0), "\n\n")
  # --- FIN DIAGNOSTIC ---

  # Fix PATH for macOS compatibility (ensure system commands are available)
  current_path <- Sys.getenv("PATH")
  if (!grepl("/bin:", current_path) && !grepl("/usr/bin:", current_path)) {
    Sys.setenv(PATH = paste("/bin:/usr/bin:", current_path, sep = ""))
  }

  cat("\n==============================\n")
  cat("[TABLE 1] Disasters, exports and poor countries (pooled)\n")
  cat("==============================\n")
  # Table 1 - All products (colonnes 1-3)
  table1_all_models <- list()
  for (i in seq_along(metric_table1_all)) {
    var <- metric_table1_all[i]
    if (var %in% names(data_all_sig)) {
      # Créer formule dynamique avec nom de variable correct
      formula_str <- paste0("d_ln_export_value ~ ", var, " + I(", var, " * is_poor_country) + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
      table1_all_models[[var]] <- lm(as.formula(formula_str), data = data_all_sig)
    } else {
      table1_all_models[[var]] <- NULL
    }
  }

  # Table 1 - Agriculture (colonnes 4-6)
  table1_agri_models <- list()
  for (i in seq_along(metric_table1_agri)) {
    var <- metric_table1_agri[i]
    if (var %in% names(data_agri_sig)) {
      # Créer formule dynamique avec nom de variable correct
      formula_str <- paste0("d_ln_export_value ~ ", var, " + I(", var, " * is_poor_country) + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
      table1_agri_models[[var]] <- lm(as.formula(formula_str), data = data_agri_sig)
    } else {
      table1_agri_models[[var]] <- NULL
    }
  }

  # Combine models and variable names for Table 1
  table1_models <- c(table1_all_models, table1_agri_models)
  table1_vars <- c(metric_table1_all, metric_table1_agri)
  table1_names <- table1_vars
  table1_types <- c(rep("All", length(metric_table1_all)), rep("Agriculture", length(metric_table1_agri)))
  # Définir les variables d'interaction pour Table 1
  table1_inter_vars <- c(
    paste0("I(", metric_table1_all, " * is_poor_country)"),
    paste0("I(", metric_table1_agri, " * is_poor_country)")
  )

  # --- Helper functions for table formatting (ensure they exist before use) ---
  format_csv <- function(x) {
    if (is.null(x) || all(is.na(x))) return(NA)
    if (is.list(x)) x <- unlist(x)
    # Retourner seulement la première valeur (coefficient) au lieu de concaténer
    if (length(x) > 0) return(x[1])
    return(NA)
  }
  
  # Nouvelle fonction pour formater proprement les coefficients (simple, sans newlines)
  format_coeff_clean <- function(coef, se, pval) {
    if (any(is.na(c(coef, se, pval)))) return("Model failed")
    
    # Étoiles de significativité
    stars <- ""
    if (pval < 0.001) stars <- "***"
    else if (pval < 0.01) stars <- "**"
    else if (pval < 0.05) stars <- "*"
    else if (pval < 0.1) stars <- "°"
    
    # Format simple: coefficient avec étoiles (pas de newlines)
    sprintf("%.6f%s", coef, stars)
  }
  # Nouvelle version robuste de extract_stats avec matching partiel et debug
  extract_stats <- function(model, var) {
    if (is.null(model)) return(NA)
    coefs <- summary(model)$coefficients
    # Matching exact
    if (var %in% rownames(coefs)) {
      est <- coefs[var, 1]
      se <- coefs[var, 2]
      pval <- coefs[var, 4]
      return(c(est, se, pval))
    } else {
      # Matching partiel
      matches <- grep(var, rownames(coefs), value = TRUE, fixed = TRUE)
      # On ne garde que les matches qui existent vraiment dans coefs
      matches <- matches[matches %in% rownames(coefs) & nzchar(matches)]
      if (length(matches) > 0) {
        est <- coefs[matches[1], 1]
        se <- coefs[matches[1], 2]
        pval <- coefs[matches[1], 4]
        cat(sprintf(
          "[DEBUG] Coefficient '%s' non trouvé, matching partiel sur '%s' → '%s'\n",
          var, var, matches[1]
        ))
        c(est, se, pval)
      } else {
        cat(sprintf("[DEBUG] Coefficient '%s' non trouvé", var))
        NA
      }
    }
  }

  cat("[TABLE 1] Construction de table1_df\n")
  table1_df <- make_table_df(
    vars = table1_vars,
    models = table1_models,
    types = table1_types,
    names = table1_names,
    inter_vars = table1_inter_vars
  )
  print(table1_df)
  # Le nom du fichier de sortie inclut la métrique principale (concaténée)
  metric_id <- paste(table1_vars, collapse="-")
  table1_filename <- paste0("table1_disasters_exports_poor_", period_str, "_", metric_id, ".csv")
  save_table_csv(table1_df, table1_filename)
  cat("[INFO] Table 1 sauvegardée sous:", file.path(TABLES_DIR, table1_filename), "\n")
  cat("[END] Table 1\n--------------------\n")

  # --- TABLE 2 ---
  cat("\n==============================\n")
  cat("[TABLE 2] Types × poor countries\n")
  cat("==============================\n")
  # === GÉNÉRATION DES MODÈLES POUR TABLES 2 ET 3 ===
  # Table 2: Types de catastrophes × pays pauvres (agriculture)
  # ANALYSE DÉSAGRÉGÉE : 2 variables par type (Occurrence + Human Deaths)
  disaster_types_t2t3 <- c("flood", "storm", "earthquake", "extreme_temperature")
  
  # Variables pour Table 2 : Occurrence ET Deaths pour chaque type
  table2_vars_occurrence <- paste0("ln_", disaster_types_t2t3, "_occurrence")
  table2_vars_deaths <- paste0("ln_", disaster_types_t2t3, "_deaths")
  table2_vars <- c()
  # Entrelacement : flood_occ, flood_deaths, storm_occ, storm_deaths, etc.
  for (i in seq_along(disaster_types_t2t3)) {
    table2_vars <- c(table2_vars, table2_vars_occurrence[i], table2_vars_deaths[i])
  }
  
  # Variables d'interaction correspondantes
  table2_inter <- c()
  for (i in seq_along(disaster_types_t2t3)) {
    table2_inter <- c(table2_inter, 
                      paste0("I(", table2_vars_occurrence[i], " * is_poor_country)"),
                      paste0("I(", table2_vars_deaths[i], " * is_poor_country)"))
  }
  
  table2_models <- list()
  
  for (i in seq_along(table2_vars)) {
    var <- table2_vars[i]
    if (var %in% names(data_agri)) {
      formula_str <- paste0("d_ln_export_value ~ ", var, " + I(", var, " * is_poor_country) + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
      table2_models[[var]] <- tryCatch(lm(as.formula(formula_str), data = data_agri), error = function(e) NULL)
    } else {
      cat("[WARN] Variable manquante pour Table 2:", var, "\n")
      table2_models[[var]] <- NULL
    }
  }
  
  # Table 3: Types de catastrophes × petits pays (agriculture)
  table3_vars <- paste0("ln_", disaster_types_t2t3, "_occurrence")
  table3_inter <- paste0("I(ln_", disaster_types_t2t3, "_occurrence * is_small_country)")
  table3_models <- list()
  
  for (i in seq_along(disaster_types_t2t3)) {
    var <- table3_vars[i]
    if (var %in% names(data_agri)) {
      formula_str <- paste0("d_ln_export_value ~ ", var, " + I(", var, " * is_small_country) + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
      table3_models[[var]] <- tryCatch(lm(as.formula(formula_str), data = data_agri), error = function(e) NULL)
    } else {
      table3_models[[var]] <- NULL
    }
  }

  if (exists("table2_models") && exists("table2_vars") && exists("table2_inter")) {
    cat("[TABLE 2] Construction de table2_df\n")
    table2_df <- make_table_df(
      vars = table2_vars,
      models = table2_models,
      inter_vars = table2_inter
    )
    print(table2_df)
    # Nom de fichier simplifié
    table2_filename <- paste0("table2_types_poor_", period_str, "_ln_flood_occurrence_ln_storm_occurrence_ln_earthquake_occurrence_ln_extreme_temperature_occurrence.csv")
    save_table_csv(table2_df, table2_filename)
    cat("[INFO] Table 2 sauvegardée sous:", file.path(TABLES_DIR, table2_filename), "\n")
    # NOTE: Génération LaTeX désormais gérée par memoire/csv_to_latex.py
  } else {
    cat("[TABLE 2][WARN] Modèles ou variables manquants, table non générée.\n")
  }
  cat("[END] Table 2\n--------------------\n")

  # --- TABLE 3 ---
  cat("\n==============================\n")
  cat("[TABLE 3] Types × small countries\n")
  cat("==============================\n")
  if (exists("table3_models") && exists("table3_vars") && exists("table3_inter")) {
    cat("[TABLE 3] Construction de table3_df\n")
    table3_df <- make_table_df(
      vars = table3_vars,
      models = table3_models,
      inter_vars = table3_inter
    )
    print(table3_df)
    # Ajout du label de métrique dans le nom du fichier
    table3_metric_label <- if (exists("table3_metric_label")) table3_metric_label else paste(table3_vars, collapse="_")
    save_table_csv(table3_df, paste0("table3_types_small_", period_str, "_", table3_metric_label, ".csv"))
    cat("[INFO] Table 3 sauvegardée sous:", file.path(TABLES_DIR, paste0("table3_types_small_", period_str, "_", table3_metric_label, ".csv")), "\n")
  } else {
    cat("[TABLE 3][WARN] Modèles ou variables manquants, table non générée.\n")
  }
  cat("[END] Table 3\n--------------------\n")

  # === TABLE 5: Extreme disasters (top 10% human damage, EM-DAT) ===
  cat("\n==============================\n")
  cat("[TABLE 5] Extreme disasters (top 10% human damage, EM-DAT)\n")
  cat("==============================\n")
  
  # Variables d'interaction pour catastrophes extrêmes EM-DAT
  disaster_types <- c("flood", "storm", "earthquake", "extreme_temperature")
  table5_vars <- rep(disaster_types, each=2)
  table5_type <- rep(c("Poor", "Small"), times=length(disaster_types))
  table5_models <- lapply(seq_along(table5_vars), function(i) {
    v <- table5_vars[i]
    type <- table5_type[i]
    extreme_var <- paste0("extreme_", v, "_emdat")
    ln_occurrence_var <- paste0("ln_", v, "_occurrence")
    
    if (extreme_var %in% names(data_agri) && ln_occurrence_var %in% names(data_agri)) {
      if (type == "Poor") {
        formula_str <- paste0("d_ln_export_value ~ ", extreme_var, " * is_poor_country + ", ln_occurrence_var, " + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
        tryCatch(lm(as.formula(formula_str), data = data_agri), error = function(e) NULL)
      } else {
        formula_str <- paste0("d_ln_export_value ~ ", extreme_var, " * is_small_country + ", ln_occurrence_var, " + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
        tryCatch(lm(as.formula(formula_str), data = data_agri), error = function(e) NULL)
      }
    } else {
      NULL
    }
  })
  
  # Construction propre de la table 5 (comme dans 04b)
  table5_main_effects <- character(length(table5_models))
  table5_interactions <- character(length(table5_models))
  table5_r2 <- numeric(length(table5_models))
  table5_obs <- integer(length(table5_models))
  
  for (i in seq_along(table5_models)) {
    model <- table5_models[[i]]
    v <- table5_vars[i]
    type <- table5_type[i]
    
    if (is.null(model)) {
      table5_main_effects[i] <- "Model failed"
      table5_interactions[i] <- "Model failed" 
      table5_r2[i] <- NA
      table5_obs[i] <- 0
      next
    }
    
    # Effet principal
    main_var <- paste0("extreme_", v, "_emdat")
    main_stats <- extract_stats(model, main_var)
    if (all(!is.na(main_stats))) {
      table5_main_effects[i] <- format_coeff_clean(main_stats[1], main_stats[2], main_stats[3])
    } else {
      table5_main_effects[i] <- "Not found"
    }
    
    # Interaction
    inter_var <- if (type == "Poor") {
      paste0("extreme_", v, "_emdat:is_poor_country")
    } else {
      paste0("extreme_", v, "_emdat:is_small_country")
    }
    inter_stats <- extract_stats(model, inter_var)
    if (all(!is.na(inter_stats))) {
      table5_interactions[i] <- format_coeff_clean(inter_stats[1], inter_stats[2], inter_stats[3])
    } else {
      table5_interactions[i] <- "Not found"
    }
    
    # R² et observations
    table5_r2[i] <- round(summary(model)$r.squared, 4)
    table5_obs[i] <- nobs(model)
  }

  table5_df <- data.frame(
    Disaster = table5_vars,
    Type = table5_type,
    Main = table5_main_effects,
    Interaction = table5_interactions,
    R2 = table5_r2,
    Observations = table5_obs,
    FixedEffects = rep("Year, Country×Product, Product×Year", length(table5_vars)),
    stringsAsFactors = FALSE
  )
  
  print(table5_df)
  save_table_csv(table5_df, paste0("table5_extreme_emdat_", period_str, ".csv"))
  cat("[INFO] Table 5 sauvegardée sous:", file.path(TABLES_DIR, paste0("table5_extreme_emdat_", period_str, ".csv")), "\n")
  cat("[END] Table 5\n--------------------\n")

  # === TABLE 6: Extreme disasters (top 10% physical damage, GeoMet) ===
  cat("\n==============================\n")
  cat("[TABLE 6] Extreme disasters (top 10% physical damage, GeoMet)\n")
  cat("==============================\n")
  
  # Variables d'interaction pour catastrophes extrêmes GeoMet
  table6_vars <- rep(disaster_types, each=2)
  table6_type <- rep(c("Poor", "Small"), times=length(disaster_types))
  table6_models <- lapply(seq_along(table6_vars), function(i) {
    v <- table6_vars[i]
    type <- table6_type[i]
    extreme_var <- paste0("extreme_", v, "_geomet")
    ln_occurrence_var <- paste0("ln_", v, "_occurrence")
    
    if (extreme_var %in% names(data_agri) && ln_occurrence_var %in% names(data_agri)) {
      if (type == "Poor") {
        formula_str <- paste0("d_ln_export_value ~ ", extreme_var, " * is_poor_country + ", ln_occurrence_var, " + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
        tryCatch(lm(as.formula(formula_str), data = data_agri), error = function(e) NULL)
      } else {
        formula_str <- paste0("d_ln_export_value ~ ", extreme_var, " * is_small_country + ", ln_occurrence_var, " + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
        tryCatch(lm(as.formula(formula_str), data = data_agri), error = function(e) NULL)
      }
    } else {
      NULL
    }
  })
  
  # Construction propre de la table 6 (comme dans 04b)
  table6_main_effects <- character(length(table6_models))
  table6_interactions <- character(length(table6_models))
  table6_r2 <- numeric(length(table6_models))
  table6_obs <- integer(length(table6_models))
  
  for (i in seq_along(table6_models)) {
    model <- table6_models[[i]]
    v <- table6_vars[i]
    type <- table6_type[i]
    
    if (is.null(model)) {
      table6_main_effects[i] <- "Model failed"
      table6_interactions[i] <- "Model failed" 
      table6_r2[i] <- NA
      table6_obs[i] <- 0
      next
    }
    
    # Effet principal
    main_var <- paste0("extreme_", v, "_geomet")
    main_stats <- extract_stats(model, main_var)
    if (all(!is.na(main_stats))) {
      table6_main_effects[i] <- format_coeff_clean(main_stats[1], main_stats[2], main_stats[3])
    } else {
      table6_main_effects[i] <- "Not found"
    }
    
    # Interaction
    inter_var <- if (type == "Poor") {
      paste0("extreme_", v, "_geomet:is_poor_country")
    } else {
      paste0("extreme_", v, "_geomet:is_small_country")
    }
    inter_stats <- extract_stats(model, inter_var)
    if (all(!is.na(inter_stats))) {
      table6_interactions[i] <- format_coeff_clean(inter_stats[1], inter_stats[2], inter_stats[3])
    } else {
      table6_interactions[i] <- "Not found"
    }
    
    # R² et observations
    table6_r2[i] <- round(summary(model)$r.squared, 4)
    table6_obs[i] <- nobs(model)
  }

  table6_df <- data.frame(
    Disaster = table6_vars,
    Type = table6_type,
    Main = table6_main_effects,
    Interaction = table6_interactions,
    R2 = table6_r2,
    Observations = table6_obs,
    FixedEffects = rep("Year, Country×Product, Product×Year", length(table6_vars)),
    stringsAsFactors = FALSE
  )
  
  print(table6_df)
  save_table_csv(table6_df, paste0("table6_extreme_geomet_", period_str, ".csv"))
  cat("[INFO] Table 6 sauvegardée sous:", file.path(TABLES_DIR, paste0("table6_extreme_geomet_", period_str, ".csv")), "\n")
  cat("[END] Table 6\n--------------------\n")

  # === TABLE 4: Comparison EM-DAT vs GeoMet (synthesis) ===
  cat("\n==============================\n")
  cat("[TABLE 4] Comparison EM-DAT vs GeoMet (synthesis)\n")
  cat("==============================\n")
  
  # Création d'une table de synthèse comparant les résultats EM-DAT et GeoMet
  disaster_types_t4 <- c("flood", "storm", "earthquake", "extreme_temperature")
  table4_comparison <- data.frame()
  
  for (dtype in disaster_types_t4) {
    # Extraire les coefficients principaux des Tables 5 et 6 pour ce type de catastrophe
    table5_rows <- table5_df[table5_df$Disaster == dtype, ]
    table6_rows <- table6_df[table6_df$Disaster == dtype, ]
    
    if (nrow(table5_rows) > 0 && nrow(table6_rows) > 0) {
      for (country_type in c("Poor", "Small")) {
        t5_row <- table5_rows[table5_rows$Type == country_type, ]
        t6_row <- table6_rows[table6_rows$Type == country_type, ]
        
        if (nrow(t5_row) > 0 && nrow(t6_row) > 0) {
          # Extraire les coefficients d'interaction
          # Fonction pour extraire le coefficient numérique à partir du format formatté
          extract_coef <- function(formatted_str) {
            # Pour le nouveau format propre (comme dans 04b)
            if (grepl("\n", formatted_str)) {
              parts <- strsplit(formatted_str, "\n")[[1]]
              if (length(parts) > 0) {
                num_str <- gsub("[^0-9.-]", "", parts[1])
                return(as.numeric(num_str))
              }
            }
            return(NA)
          }
          
          t5_coef <- extract_coef(t5_row$Interaction)
          t6_coef <- extract_coef(t6_row$Interaction)
          
          table4_comparison <- rbind(table4_comparison, data.frame(
            Disaster = dtype,
            Type = country_type,
            EMDAT_Coef = t5_coef,
            GeoMet_Coef = t6_coef,
            Difference = t5_coef - t6_coef,
            Period = period_str,
            stringsAsFactors = FALSE
          ))
        }
      }
    }
  }
  
  print(table4_comparison)
  save_table_csv(table4_comparison, paste0("table4_comparison_emdat_geomet_", period_str, ".csv"))
  cat("[INFO] Table 4 sauvegardée sous:", file.path(TABLES_DIR, paste0("table4_comparison_emdat_geomet_", period_str, ".csv")), "\n")
  cat("[END] Table 4\n--------------------\n")

  # === TABLE 7: Comparison extreme disasters (synthesis) ===
  cat("\n==============================\n")
  cat("[TABLE 7] Comparison extreme disasters (synthesis)\n")
  cat("==============================\n")
  
  # Table de synthèse des catastrophes extrêmes
  table7_summary <- data.frame()
  
  for (dtype in disaster_types_t4) {
    # Statistiques descriptives des variables extrêmes
    extreme_emdat_var <- paste0("extreme_", dtype, "_emdat")
    extreme_geomet_var <- paste0("extreme_", dtype, "_geomet")
    
    if (extreme_emdat_var %in% names(data_agri) && extreme_geomet_var %in% names(data_agri)) {
      emdat_events <- sum(data_agri[[extreme_emdat_var]], na.rm = TRUE)
      geomet_events <- sum(data_agri[[extreme_geomet_var]], na.rm = TRUE)
      total_obs <- nrow(data_agri)
      
      table7_summary <- rbind(table7_summary, data.frame(
        Disaster = dtype,
        EMDAT_Extreme_Events = emdat_events,
        EMDAT_Percentage = round((emdat_events / total_obs) * 100, 2),
        GeoMet_Extreme_Events = geomet_events,
        GeoMet_Percentage = round((geomet_events / total_obs) * 100, 2),
        Total_Observations = total_obs,
        Period = period_str,
        stringsAsFactors = FALSE
      ))
    }
  }
  
  print(table7_summary)
  save_table_csv(table7_summary, paste0("table7_extreme_disasters_summary_", period_str, ".csv"))
  cat("[INFO] Table 7 sauvegardée sous:", file.path(TABLES_DIR, paste0("table7_extreme_disasters_summary_", period_str, ".csv")), "\n")
  cat("[END] Table 7\n--------------------\n")

  # === TABLE A1: Extreme disasters (top 20% human damage) - Appendix ===
  cat("\n==============================\n")
  cat("[TABLE A1] Extreme disasters (top 20% human damage) - Appendix\n")
  cat("==============================\n")
  
  # Recréer les variables extrêmes avec seuil 20% (top 80% quantile)
  for (dtype in disaster_types_vars) {
    death_col <- paste0(dtype, "_deaths")
    extreme_var_a1 <- paste0("extreme_", dtype, "_emdat_20p")
    if (death_col %in% names(data)) {
      threshold_80p <- quantile(data[[death_col]], 0.8, na.rm=TRUE)
      data[[extreme_var_a1]] <- ifelse(data[[death_col]] >= threshold_80p & data[[death_col]] > 0, 1, 0)
      data[[extreme_var_a1]][is.na(data[[death_col]])] <- 0
    }
  }
  
  # Recréer data_agri avec les nouvelles variables
  data_agri_a1 <- subset(data, is_agri)
  
  tableA1_vars <- rep(disaster_types, each=2)
  tableA1_type <- rep(c("Poor", "Small"), times=length(disaster_types))
  tableA1_models <- lapply(seq_along(tableA1_vars), function(i) {
    v <- tableA1_vars[i]
    type <- tableA1_type[i]
    extreme_var <- paste0("extreme_", v, "_emdat_20p")
    ln_occurrence_var <- paste0("ln_", v, "_occurrence")
    
    if (extreme_var %in% names(data_agri_a1) && ln_occurrence_var %in% names(data_agri_a1)) {
      if (type == "Poor") {
        formula_str <- paste0("d_ln_export_value ~ ", extreme_var, " * is_poor_country + ", ln_occurrence_var, " + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
        tryCatch(lm(as.formula(formula_str), data = data_agri_a1), error = function(e) NULL)
      } else {
        formula_str <- paste0("d_ln_export_value ~ ", extreme_var, " * is_small_country + ", ln_occurrence_var, " + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
        tryCatch(lm(as.formula(formula_str), data = data_agri_a1), error = function(e) NULL)
      }
    } else {
      NULL
    }
  })
  
  # Construction propre de la table A1 (comme dans 04b)
  tableA1_main_effects <- character(length(tableA1_models))
  tableA1_interactions <- character(length(tableA1_models))
  tableA1_r2 <- numeric(length(tableA1_models))
  tableA1_obs <- integer(length(tableA1_models))
  
  for (i in seq_along(tableA1_models)) {
    model <- tableA1_models[[i]]
    v <- tableA1_vars[i]
    type <- tableA1_type[i]
    
    if (is.null(model)) {
      tableA1_main_effects[i] <- "Model failed"
      tableA1_interactions[i] <- "Model failed" 
      tableA1_r2[i] <- NA
      tableA1_obs[i] <- 0
      next
    }
    
    # Effet principal
    main_var <- paste0("extreme_", v, "_emdat_20p")
    main_stats <- extract_stats(model, main_var)
    if (all(!is.na(main_stats))) {
      tableA1_main_effects[i] <- format_coeff_clean(main_stats[1], main_stats[2], main_stats[3])
    } else {
      tableA1_main_effects[i] <- "Not found"
    }
    
    # Interaction
    inter_var <- if (type == "Poor") {
      paste0("extreme_", v, "_emdat_20p:is_poor_country")
    } else {
      paste0("extreme_", v, "_emdat_20p:is_small_country")
    }
    inter_stats <- extract_stats(model, inter_var)
    if (all(!is.na(inter_stats))) {
      tableA1_interactions[i] <- format_coeff_clean(inter_stats[1], inter_stats[2], inter_stats[3])
    } else {
      tableA1_interactions[i] <- "Not found"
    }
    
    # R² et observations
    tableA1_r2[i] <- round(summary(model)$r.squared, 4)
    tableA1_obs[i] <- nobs(model)
  }

  tableA1_df <- data.frame(
    Disaster = tableA1_vars,
    Type = tableA1_type,
    Main = tableA1_main_effects,
    Interaction = tableA1_interactions,
    R2 = tableA1_r2,
    Observations = tableA1_obs,
    FixedEffects = rep("Year, Country×Product, Product×Year", length(tableA1_vars)),
    stringsAsFactors = FALSE
  )
  
  print(tableA1_df)
  save_table_csv(tableA1_df, paste0("tableA1_extreme_emdat_20p_", period_str, ".csv"))
  cat("[INFO] Table A1 sauvegardée sous:", file.path(TABLES_DIR, paste0("tableA1_extreme_emdat_20p_", period_str, ".csv")), "\n")
  cat("[END] Table A1\n--------------------\n")

  # === TABLE A2: Extreme disasters (top 20% physical damage) - Appendix ===
  cat("\n==============================\n")
  cat("[TABLE A2] Extreme disasters (top 20% physical damage) - Appendix\n")
  cat("==============================\n")
  
  # Variables extrêmes GeoMet avec seuil 20% (top 80% quantile)
  for (dtype in disaster_types_vars) {
    intensity_col <- paste0(dtype, "_intensity")
    extreme_var_a2 <- paste0("extreme_", dtype, "_geomet_20p")
    if (intensity_col %in% names(data)) {
      threshold_80p <- quantile(data[[intensity_col]], 0.8, na.rm=TRUE)
      data[[extreme_var_a2]] <- ifelse(data[[intensity_col]] >= threshold_80p & data[[intensity_col]] > 0, 1, 0)
      data[[extreme_var_a2]][is.na(data[[intensity_col]])] <- 0
    }
  }
  
  # Recréer data_agri avec les nouvelles variables
  data_agri_a2 <- subset(data, is_agri)
  
  tableA2_vars <- rep(disaster_types, each=2)
  tableA2_type <- rep(c("Poor", "Small"), times=length(disaster_types))
  tableA2_models <- lapply(seq_along(tableA2_vars), function(i) {
    v <- tableA2_vars[i]
    type <- tableA2_type[i]
    extreme_var <- paste0("extreme_", v, "_geomet_20p")
    ln_occurrence_var <- paste0("ln_", v, "_occurrence")
    
    if (extreme_var %in% names(data_agri_a2) && ln_occurrence_var %in% names(data_agri_a2)) {
      if (type == "Poor") {
        formula_str <- paste0("d_ln_export_value ~ ", extreme_var, " * is_poor_country + ", ln_occurrence_var, " + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
        tryCatch(lm(as.formula(formula_str), data = data_agri_a2), error = function(e) NULL)
      } else {
        formula_str <- paste0("d_ln_export_value ~ ", extreme_var, " * is_small_country + ", ln_occurrence_var, " + d_ln_population + factor(year) + factor(iso3) + factor(product_code)")
        tryCatch(lm(as.formula(formula_str), data = data_agri_a2), error = function(e) NULL)
      }
    } else {
      NULL
    }
  })
  
  # Construction propre de la table A2 (comme dans 04b)
  tableA2_main_effects <- character(length(tableA2_models))
  tableA2_interactions <- character(length(tableA2_models))
  tableA2_r2 <- numeric(length(tableA2_models))
  tableA2_obs <- integer(length(tableA2_models))
  
  for (i in seq_along(tableA2_models)) {
    model <- tableA2_models[[i]]
    v <- tableA2_vars[i]
    type <- tableA2_type[i]
    
    if (is.null(model)) {
      tableA2_main_effects[i] <- "Model failed"
      tableA2_interactions[i] <- "Model failed" 
      tableA2_r2[i] <- NA
      tableA2_obs[i] <- 0
      next
    }
    
    # Effet principal
    main_var <- paste0("extreme_", v, "_geomet_20p")
    main_stats <- extract_stats(model, main_var)
    if (all(!is.na(main_stats))) {
      tableA2_main_effects[i] <- format_coeff_clean(main_stats[1], main_stats[2], main_stats[3])
    } else {
      tableA2_main_effects[i] <- "Not found"
    }
    
    # Interaction
    inter_var <- if (type == "Poor") {
      paste0("extreme_", v, "_geomet_20p:is_poor_country")
    } else {
      paste0("extreme_", v, "_geomet_20p:is_small_country")
    }
    inter_stats <- extract_stats(model, inter_var)
    if (all(!is.na(inter_stats))) {
      tableA2_interactions[i] <- format_coeff_clean(inter_stats[1], inter_stats[2], inter_stats[3])
    } else {
      tableA2_interactions[i] <- "Not found"
    }
    
    # R² et observations
    tableA2_r2[i] <- round(summary(model)$r.squared, 4)
    tableA2_obs[i] <- nobs(model)
  }

  tableA2_df <- data.frame(
    Disaster = tableA2_vars,
    Type = tableA2_type,
    Main = tableA2_main_effects,
    Interaction = tableA2_interactions,
    R2 = tableA2_r2,
    Observations = tableA2_obs,
    FixedEffects = rep("Year, Country×Product, Product×Year", length(tableA2_vars)),
    stringsAsFactors = FALSE
  )
  
  print(tableA2_df)
  save_table_csv(tableA2_df, paste0("tableA2_extreme_geomet_20p_", period_str, ".csv"))
  cat("[INFO] Table A2 sauvegardée sous:", file.path(TABLES_DIR, paste0("tableA2_extreme_geomet_20p_", period_str, ".csv")), "\n")
  cat("[END] Table A2\n--------------------\n")

  # =====================
  # TABLES AVANCÉES (ex-04b, désormais intégrées)
  # =====================
  criteria <- list(
    list(name = "all_events", 
         label = "Tous événements", 
         description = "Toutes observations",
         filter_func = function(d) d),
    list(name = "significant_events", 
         label = "Événements significatifs", 
         description = "Au moins 1 mort OU 2+ événements",
         filter_func = function(d) {
           d[d$sum_deaths >= 1 | d$sum_events >= 2, ]
         })
  )
  for (criterion in criteria) {
    cat("\n==============================\n")
    cat("[TABLES AVANCÉES] Période:", period_str, "Critère:", criterion$label, "\n")
    filtered_data <- criterion$filter_func(data)
    cat("   Observations après filtrage:", nrow(filtered_data), "\n")
    if (nrow(filtered_data) < 50) {
      cat("   ⚠️ Trop peu d'observations, combinaison ignorée\n\n")
      next
    }
    # --- TABLE 2 avancée : Types de catastrophes par niveau de richesse ---
    disaster_types_adv <- tolower(gsub(" ", "_", config$DISASTER_TYPES))
    log_vars <- c("ln_total_occurrence", "ln_total_deaths", paste0("ln_", disaster_types_adv, "_count"))
    group_vars <- c("income_group_internal", "size_group")
    for (v in log_vars) {
      if (!v %in% names(filtered_data)) cat("[WARN] Variable manquante :", v, "\n")
    }
    for (v in group_vars) {
      if (!v %in% names(filtered_data)) cat("[WARN] Variable manquante :", v, "\n")
    }
    poor_data <- filtered_data[filtered_data$income_group_internal == "Low", , drop=FALSE]
    rich_data <- filtered_data[filtered_data$income_group_internal == "High", , drop=FALSE]
    models_poor <- list(); models_rich <- list()
    for (i in 1:4) {
      dtype <- disaster_types_adv[i]
      formula_str <- paste0("d_ln_export_value ~ ln_", dtype, "_count")
      if (nrow(poor_data) > 0) {
        models_poor[[i]] <- tryCatch(lm(as.formula(formula_str), data = poor_data), error=function(e) NULL)
      } else { models_poor[[i]] <- NULL }
      if (nrow(rich_data) > 0) {
        models_rich[[i]] <- tryCatch(lm(as.formula(formula_str), data = rich_data), error=function(e) NULL)
      } else { models_rich[[i]] <- NULL }
    }
    if (length(models_poor) < 4) models_poor[(length(models_poor)+1):4] <- list(NULL)
    if (length(models_rich) < 4) models_rich[(length(models_rich)+1):4] <- list(NULL)
    table2_out <- data.frame(
      Group = c(rep("Low income", 4), rep("High income", 4)),
      Disaster = rep(disaster_types_adv, 2),
      Coef = c(sapply(models_poor, function(m) if (!is.null(m)) coef(m)[2] else NA),
               sapply(models_rich, function(m) if (!is.null(m)) coef(m)[2] else NA)),
      SE = c(sapply(models_poor, function(m) if (!is.null(m)) summary(m)$coefficients[2,2] else NA),
             sapply(models_rich, function(m) if (!is.null(m)) summary(m)$coefficients[2,2] else NA)),
      Pval = c(sapply(models_poor, function(m) if (!is.null(m)) summary(m)$coefficients[2,4] else NA),
               sapply(models_rich, function(m) if (!is.null(m)) summary(m)$coefficients[2,4] else NA)),
      N = c(rep(nrow(poor_data), 4), rep(nrow(rich_data), 4))
    )
    fname2 <- paste0("table2_types_income_", period_str, "_", criterion$name, ".csv")
    print(table2_out)
    write.csv(table2_out, file.path(TABLES_DIR, fname2), row.names=FALSE)
    cat("[INFO] Table 2 avancée sauvegardée :", fname2, "\n")
    # --- TABLE 3 avancée : Types de catastrophes par taille de pays ---
    small_data <- filtered_data[filtered_data$size_group == "Small", , drop=FALSE]
    large_data <- filtered_data[filtered_data$size_group == "Large", , drop=FALSE]
    models_small <- list(); models_large <- list()
    for (i in 1:4) {
      dtype <- disaster_types_adv[i]
      formula_str <- paste0("d_ln_export_value ~ ln_", dtype, "_count")
      if (nrow(small_data) > 0) {
        models_small[[i]] <- tryCatch(lm(as.formula(formula_str), data = small_data), error=function(e) NULL)
      } else { models_small[[i]] <- NULL }
      if (nrow(large_data) > 0) {
        models_large[[i]] <- tryCatch(lm(as.formula(formula_str), data = large_data), error=function(e) NULL)
      } else { models_large[[i]] <- NULL }
    }
    if (length(models_small) < 4) models_small[(length(models_small)+1):4] <- list(NULL)
    if (length(models_large) < 4) models_large[(length(models_large)+1):4] <- list(NULL)
    table3_out <- data.frame(
      Group = c(rep("Small", 4), rep("Large", 4)),
      Disaster = rep(disaster_types_adv, 2),
      Coef = c(sapply(models_small, function(m) if (!is.null(m)) coef(m)[2] else NA),
               sapply(models_large, function(m) if (!is.null(m)) coef(m)[2] else NA)),
      SE = c(sapply(models_small, function(m) if (!is.null(m)) summary(m)$coefficients[2,2] else NA),
             sapply(models_large, function(m) if (!is.null(m)) summary(m)$coefficients[2,2] else NA)),
      Pval = c(sapply(models_small, function(m) if (!is.null(m)) summary(m)$coefficients[2,4] else NA),
               sapply(models_large, function(m) if (!is.null(m)) summary(m)$coefficients[2,4] else NA)),
      N = c(rep(nrow(small_data), 4), rep(nrow(large_data), 4))
    )
    print(table3_out)
    fname3 <- paste0("table3_types_size_", period_str, "_", criterion$name, ".csv")
    write.csv(table3_out, file.path(TABLES_DIR, fname3), row.names=FALSE)
    cat("[INFO] Table 3 avancée sauvegardée :", fname3, "\n")
  }

  # Résumé et sauvegarde modèles (inchangé)
  all_models <- list(
    table1_models = table1_models,
    table5_models = table5_models,
    table6_models = table6_models,
    data_all = data_all,
    data_agri = data_agri,
    analysis_date = Sys.Date()
  )
  if (exists("table2_models")) all_models$table2_models <- table2_models
  if (exists("table3_models")) all_models$table3_models <- table3_models
  saveRDS(all_models, file.path(RESULTS_DIR, "all_article_models.rds"))
  results_summary <- data.frame(
    Table = c("Table 1", "Table 2", "Table 3", "Table 5", "Table 6"),
    Description = c("Pooled disasters", "Types × poor", "Types × small", "Extreme EM-DAT", "Extreme GeoMet"),
    N_Models = c(
      length(table1_models),
      if (exists("table2_models")) length(table2_models) else 0,
      if (exists("table3_models")) length(table3_models) else 0,
      length(table5_models),
      length(table6_models)
    ),
    Period = period_str,
    Status = "Complete"
  )
  print(results_summary)
  cat("\n==============================\n")
  cat("[RÉSUMÉ] Analyse économétrique terminée pour période ", period_str, "\n")
  cat("==============================\n")}
# Fin du script