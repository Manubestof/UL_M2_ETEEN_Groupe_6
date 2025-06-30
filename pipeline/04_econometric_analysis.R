#!/usr/local/bin/Rscript
# ÉTAPE 4 : ANALYSE ÉCONOMÉTRIQUE COMPLÈTE - REPRODUCTION INTÉGRALE DE L'ARTICLE
# Article : El Hadri, Mizra, Rabaud (2019)
# "Natural disasters and countries exports: New insights from a new (and an old) database"
#
# REPRODUCTION EXACTE DE TOUTES LES TABLES (1-7 + A1-A2) :
# Table 1: Disasters, exports and poor countries (all four types pooled)
# Table 2: Types of disasters, agricultural exports and poor countries  
# Table 3: Types of disasters, agricultural exports and small countries
# Table 4: Comparison EM-DAT vs GeoMet (synthesis)
# Table 5: Extreme disasters (top 10% human damage, EM-DAT)
# Table 6: Extreme disasters (top 10% physical damage, GeoMet)  
# Table 7: Comparison extreme disasters (synthesis)
# Table A1: Extreme disasters (top 20% human damage) - Appendix
# Table A2: Extreme disasters (top 20% physical damage) - Appendix

# Dynamique : lecture de config.json pour piloter toute l'analyse
library(jsonlite)
library(dplyr)
library(readr)
library(tidyr)

# === Robust helper functions (define ONCE, at the top) ===
save_table_csv <- function(df, filename) {
  write.csv(df, file = file.path(TABLES_DIR, filename), row.names = FALSE)
}
make_table_df <- function(vars, models, inter_vars = NULL, types = NULL, names = NULL, fixed_effects = "Year, Country×Product, Product×Year") {
  keep <- which(!sapply(models, is.null))
  models <- models[keep]
  n <- length(models)
  # Correction : s'assurer que vars est de longueur n et ne contient pas NA
  if (!is.null(vars)) vars <- vars[keep]
  if (!is.null(types)) types <- types[keep]
  if (!is.null(names)) names <- names[keep]
  if (!is.null(inter_vars)) inter_vars <- inter_vars[keep]
  safe_extract <- function(i, fun, varname) {
    if (i > length(models) || is.null(models[[i]]) || is.null(varname) || is.na(varname) || !nzchar(varname)) return(NA)
    fun(models[[i]], varname)
  }
  # Correction : ne jamais passer NA comme nom de variable
  main <- unlist(lapply(seq_len(n), function(i) {
    v <- if (!is.null(vars) && length(vars) >= i) vars[i] else NA
    if (is.null(v) || is.na(v) || !nzchar(v)) return(NA)
    format_csv(safe_extract(i, extract_stats, v))
  }))
  interaction <- if (!is.null(inter_vars)) unlist(lapply(seq_len(n), function(i) format_csv(safe_extract(i, extract_stats, inter_vars[i])))) else rep(NA, n)
  control <- unlist(lapply(seq_len(n), function(i) format_csv(safe_extract(i, extract_stats, "d_ln_population"))))
  r2 <- unlist(lapply(models, function(m) if (!is.null(m)) round(summary(m)$r.squared,4) else NA))
  obs <- unlist(lapply(models, function(m) if (!is.null(m)) nobs(m) else NA))
  fe <- rep(fixed_effects, n)
  df <- data.frame(
    Variable = if (!is.null(names)) names else vars,
    Type = if (!is.null(types)) types else NA,
    Main = main,
    Interaction = interaction,
    Control = control,
    R2 = r2,
    Obs = obs,
    FixedEffects = fe,
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
  # --- Création des variables nécessaires à l'analyse (conformément à l'article et au mémoire) ---
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
    # Normalisation par écart-type (comme dans l'article)
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
  # --- FIN création des métriques ---

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
  # Modèles Table 1 - All products (colonnes 1-3)
  table1_all_models <- list()

  # Colonne 1: Occurrence (All products)
  if ("ln_total_occurrence" %in% names(data_all)) {
    table1_all_models$occur <- lm(d_ln_export_value ~ ln_total_occurrence + 
                                  I(ln_total_occurrence * is_poor_country) + 
                                  d_ln_population + 
                                  factor(year) + factor(iso3) + factor(product_code),
                                  data = data_all)
  }

  # Colonne 2: Deaths (All products)  
  if ("ln_total_deaths" %in% names(data_all)) {
    table1_all_models$deaths <- lm(d_ln_export_value ~ ln_total_deaths + 
                                  I(ln_total_deaths * is_poor_country) + 
                                  d_ln_population + 
                                  factor(year) + factor(iso3) + factor(product_code),
                                  data = data_all)
  }

  # Colonne 3: Index (All products)
  if ("disaster_index" %in% names(data_all)) {
    table1_all_models$index <- lm(d_ln_export_value ~ disaster_index + 
                                 I(disaster_index * is_poor_country) + 
                                 d_ln_population + 
                                 factor(year) + factor(iso3) + factor(product_code),
                                 data = data_all)
  }

  # Modèles Table 1 - Agriculture (colonnes 4-6)
  table1_agri_models <- list()

  # Colonne 4: Occurrence (Agriculture)
  if ("ln_total_occurrence" %in% names(data_agri)) {
    table1_agri_models$occur <- lm(d_ln_export_value ~ ln_total_occurrence + 
                                  I(ln_total_occurrence * is_poor_country) + 
                                  d_ln_population + 
                                  factor(year) + factor(iso3) + factor(product_code),
                                  data = data_agri)
  }

  # Colonne 5: Deaths (Agriculture)
  if ("ln_total_deaths" %in% names(data_agri)) {
    table1_agri_models$deaths <- lm(d_ln_export_value ~ ln_total_deaths + 
                                   I(ln_total_deaths * is_poor_country) + 
                                   d_ln_population + 
                                   factor(year) + factor(iso3) + factor(product_code),
                                   data = data_agri)
  }

  # Colonne 6: Index (Agriculture)
  if ("disaster_index" %in% names(data_agri)) {
    table1_agri_models$index <- lm(d_ln_export_value ~ disaster_index + 
                                  I(disaster_index * is_poor_country) + 
                                  d_ln_population + 
                                  factor(year) + factor(iso3) + factor(product_code),
                                  data = data_agri)
  }

  table1_vars <- c("ln_total_occurrence", "ln_total_deaths", "disaster_index")
  # DEBUG: Afficher les noms de coefficients pour chaque modèle Agriculture et la variable recherchée
  cat("\n[DEBUG] Coefficients et variable recherchée pour chaque modèle Agriculture (Table 1):\n")
  for (i in seq_along(table1_agri_models)) {
    mod <- table1_agri_models[[i]]
    var <- table1_vars[i]
    if (!is.null(mod)) {
      cat(paste0("  - Modèle ", names(table1_agri_models)[i], ": variable recherchée '", var, "'\n"))
      cat(paste0("    Coefs: ", paste(rownames(summary(mod)$coefficients), collapse=", "), "\n"))
    } else {
      cat(paste0("  - Modèle ", names(table1_agri_models)[i], ": NULL\n"))
    }
  }

  # Combiner tous modèles Table 1
  table1_models <- c(table1_all_models, table1_agri_models)
  # Correction : le vecteur vars doit être dupliqué pour All et Agriculture
  table1_vars <- rep(c("ln_total_occurrence", "ln_total_deaths", "disaster_index"), 2)
  table1_names <- table1_vars
  table1_types <- c(rep("All", 3), rep("Agriculture", 3))
  # --- Helper functions for table formatting (ensure they exist before use) ---
  format_csv <- function(x) {
    if (is.null(x) || all(is.na(x))) return(NA)
    if (is.list(x)) x <- unlist(x)
    paste(x, collapse = ", ")
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
        cat(sprintf("[DEBUG] Coefficient '%s' non trouvé, matching partiel sur '%s' → '%s'\n", var, var, matches[1]))
        return(c(est, se, pval))
      } else {
        cat(sprintf("[DEBUG] Coefficient '%s' non trouvé dans : %s\n", var, paste(rownames(coefs), collapse=", ")))
        return(NA)
      }
    }
  }

  cat("[TABLE 1] Construction de table1_df\n")
  table1_df <- make_table_df(
    vars = table1_vars,
    models = table1_models,
    types = table1_types,
    names = table1_names
  )
  print(table1_df)
  save_table_csv(table1_df, paste0("table1_disasters_exports_poor_", period_str, ".csv"))
  cat("[INFO] Table 1 sauvegardée sous:", file.path(TABLES_DIR, paste0("table1_disasters_exports_poor_", period_str, ".csv")), "\n")
  cat("[END] Table 1\n--------------------\n")

  # --- TABLE 2 ---
  cat("\n==============================\n")
  cat("[TABLE 2] Types × poor countries\n")
  cat("==============================\n")
  if (exists("table2_models") && exists("table2_vars") && exists("table2_inter")) {
    cat("[TABLE 2] Construction de table2_df\n")
    table2_df <- make_table_df(
      vars = table2_vars,
      models = table2_models,
      inter_vars = table2_inter
    )
    print(table2_df)
    save_table_csv(table2_df, paste0("table2_types_poor_", period_str, ".csv"))
    cat("[INFO] Table 2 sauvegardée sous:", file.path(TABLES_DIR, paste0("table2_types_poor_", period_str, ".csv")), "\n")
    # --- Génération automatique du .tex pour Table 2 ---
    tex_path <- file.path(project_root, "memoire/tables/table2_types_poor.tex")
    cat("% Table générée automatiquement depuis pipeline/04_econometric_analysis.R le ", format(Sys.time(), "%Y-%m-%d %H:%M"), "\n", file=tex_path)
    cat("\\begin{table}[h]\n\\centering\n\\caption{Types of disasters, agricultural exports and poor countries}\n", file=tex_path, append=TRUE)
    cat("\\begin{tabular}{lcccccccc}\n\\toprule\n", file=tex_path, append=TRUE)
    # En-têtes (à adapter selon table2_df)
    write.table(t(names(table2_df)), file=tex_path, append=TRUE, sep=" & ", row.names=FALSE, col.names=FALSE, quote=FALSE)
    cat("\\\\\\midrule\n", file=tex_path, append=TRUE)
    # Lignes du tableau
    write.table(table2_df, file=tex_path, append=TRUE, sep=" & ", row.names=FALSE, col.names=FALSE, quote=FALSE)
    cat("\\\\\\bottomrule\n\\end{tabular}\n\\end{table}\n", file=tex_path, append=TRUE)
    cat("[INFO] Table 2 LaTeX générée sous:", tex_path, "\n")
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
    save_table_csv(table3_df, paste0("table3_types_small_", period_str, ".csv"))
    cat("[INFO] Table 3 sauvegardée sous:", file.path(TABLES_DIR, paste0("table3_types_small_", period_str, ".csv")), "\n")
  } else {
    cat("[TABLE 3][WARN] Modèles ou variables manquants, table non générée.\n")
  }
  cat("[END] Table 3\n--------------------\n")

  # === TABLE 5: extreme disasters (top 10% EM-DAT) ===
  cat("\n==============================\n")
  cat("[TABLE 5] Extreme disasters (top 10% EM-DAT)\n")
  cat("==============================\n")
  disaster_types <- c("flood", "storm", "earthquake", "extreme_temperature")
  table5_vars <- rep(disaster_types, each=2)
  table5_type <- rep(c("Poor", "Small"), times=length(disaster_types))
  table5_models <- lapply(seq_along(table5_vars), function(i) {
    v <- table5_vars[i]
    type <- table5_type[i]
    extreme_var <- paste0("extreme_", v, "_emdat")
    if (extreme_var %in% names(data_agri) && paste0("ln_", v, "_occurrence") %in% names(data_agri)) {
      if (type == "Poor") {
        lm(d_ln_export_value ~ get(extreme_var) * is_poor_country + get(paste0("ln_", v, "_occurrence")) + d_ln_population + factor(year) + factor(iso3) + factor(product_code), data = data_agri)
      } else {
        lm(d_ln_export_value ~ get(extreme_var) * is_small_country + get(paste0("ln_", v, "_occurrence")) + d_ln_population + factor(year) + factor(iso3) + factor(product_code), data = data_agri)
      }
    } else {
      NULL
    }
  })
  table5_main <- unlist(lapply(seq_along(table5_models), function(i) format_csv(extract_stats(table5_models[[i]], paste0("ln_", table5_vars[i], "_occurrence")))))
  table5_inter <- unlist(lapply(seq_along(table5_models), function(i) {
    v <- table5_vars[i]
    type <- table5_type[i]
    if (type=="Poor") format_csv(extract_stats(table5_models[[i]], paste0("extreme_", v, "_emdat:is_poor_country")))
    else format_csv(extract_stats(table5_models[[i]], paste0("extreme_", v, "_emdat:is_small_country")))
  }))
  table5_df <- data.frame(
    Disaster = table5_vars,
    Type = table5_type,
    Main = table5_main,
    Interaction = table5_inter,
    Control = unlist(lapply(seq_along(table5_models), function(i) format_csv(extract_stats(table5_models[[i]], "d_ln_population")))),
    R2 = unlist(lapply(table5_models, function(m) if (!is.null(m)) round(summary(m)$r.squared,4) else NA)),
    Obs = unlist(lapply(table5_models, function(m) if (!is.null(m)) nobs(m) else NA)),
    FixedEffects = rep("Year, Country×Product, Product×Year", length(table5_vars))
  )
  print(table5_df)
  save_table_csv(table5_df, paste0("table5_extreme_emdat_", period_str, ".csv"))
  cat("[INFO] Table 5 sauvegardée sous:", file.path(TABLES_DIR, paste0("table5_extreme_emdat_", period_str, ".csv")), "\n")
  cat("[END] Table 5\n--------------------\n")

  # === TABLE 6: extreme disasters (top 10% GeoMet) ===
  cat("\n==============================\n")
  cat("[TABLE 6] Extreme disasters (top 10% GeoMet)\n")
  cat("==============================\n")
  table6_vars <- rep(disaster_types, each=2)
  table6_type <- rep(c("Poor", "Small"), times=length(disaster_types))
  table6_models <- lapply(seq_along(table6_vars), function(i) {
    v <- table6_vars[i]
    type <- table6_type[i]
    extreme_var <- paste0("extreme_", v, "_geomet")
    if (extreme_var %in% names(data_agri) && paste0("ln_", v, "_occurrence") %in% names(data_agri)) {
      if (type == "Poor") {
        lm(d_ln_export_value ~ get(extreme_var) * is_poor_country + get(paste0("ln_", v, "_occurrence")) + d_ln_population + factor(year) + factor(iso3) + factor(product_code), data = data_agri)
      } else {
        lm(d_ln_export_value ~ get(extreme_var) * is_small_country + get(paste0("ln_", v, "_occurrence")) + d_ln_population + factor(year) + factor(iso3) + factor(product_code), data = data_agri)
      }
    } else {
      NULL
    }
  })
  table6_main <- unlist(lapply(seq_along(table6_models), function(i) format_csv(extract_stats(table6_models[[i]], paste0("ln_", table6_vars[i], "_occurrence")))))
  table6_inter <- unlist(lapply(seq_along(table6_models), function(i) {
    v <- table6_vars[i]
    type <- table6_type[i]
    if (type=="Poor") format_csv(extract_stats(table6_models[[i]], paste0("extreme_", v, "_geomet:is_poor_country")))
    else format_csv(extract_stats(table6_models[[i]], paste0("extreme_", v, "_geomet:is_small_country")))
  }))
  table6_df <- data.frame(
    Disaster = table6_vars,
    Type = table6_type,
    Main = table6_main,
    Interaction = table6_inter,
    Control = unlist(lapply(seq_along(table6_models), function(i) format_csv(extract_stats(table6_models[[i]], "d_ln_population")))),
    R2 = unlist(lapply(table6_models, function(m) if (!is.null(m)) round(summary(m)$r.squared,4) else NA)),
    Obs = unlist(lapply(table6_models, function(m) if (!is.null(m)) nobs(m) else NA)),
    FixedEffects = rep("Year, Country×Product, Product×Year", length(table6_vars))
  )
  print(table6_df)
  save_table_csv(table6_df, paste0("table6_extreme_geomet_", period_str, ".csv"))
  cat("[INFO] Table 6 sauvegardée sous:", file.path(TABLES_DIR, paste0("table6_extreme_geomet_", period_str, ".csv")), "\n")
  cat("[END] Table 6\n--------------------\n")

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
  print(results_summary) # Ajout : affichage du résumé dans la console
  write.csv(results_summary, file.path(RESULTS_DIR, paste0("article_reproduction_summary_", period_str, ".csv")), row.names = FALSE)
  cat("\n==============================\n")
  cat("[RÉSUMÉ] Analyse économétrique terminée pour période ", period_str, "\n")
  cat("==============================\n")
  cat("\n[INFO] Pour les analyses multi-périodes et robustesse, lancez le script 04b_multi_period_analysis.R\n")
}
# Fin du script