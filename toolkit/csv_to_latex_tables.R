# csv_to_latex_tables.R
# Convertit tous les fichiers CSV de tables en LaTeX pour intégration dans le mémoire
# Usage : Rscript toolkit/csv_to_latex_tables.R

library(tools)

# Dossier d'entrée (CSV) et de sortie (LaTeX)
tables_dir <- "pipeline/tables"  # Adapter si besoin
latex_dir <- "memoire/tables"

if (!dir.exists(latex_dir)) dir.create(latex_dir, recursive = TRUE)

# Fonction de conversion générique CSV -> LaTeX tabular
csv_to_latex <- function(csv_path, latex_path, caption = NULL, label = NULL) {
  df <- read.csv(csv_path, stringsAsFactors = FALSE)
  ncol <- ncol(df)
  col_names <- names(df)
  
  latex <- "\\begin{table}[h]\n\\centering\n"
  if (!is.null(caption)) latex <- paste0(latex, "\\caption{", caption, "}\n")
  if (!is.null(label)) latex <- paste0(latex, "\\label{", label, "}\n")
  latex <- paste0(latex, "\\begin{tabular}{", paste(rep("c", ncol), collapse = ""), "}\n\\toprule\n")
  latex <- paste0(latex, paste(col_names, collapse = " & "), " \\\\ \\midrule\n")
  for (i in 1:nrow(df)) {
    row <- df[i, ]
    latex <- paste0(latex, paste(row, collapse = " & "), " \\\\ \n")
  }
  latex <- paste0(latex, "\\bottomrule\n\\end{tabular}\n\\end{table}\n")
  writeLines(latex, latex_path)
  cat("✅ LaTeX généré:", latex_path, "\n")
}

# Conversion de tous les CSV du dossier
table_files <- list.files(tables_dir, pattern = "\\.csv$", full.names = TRUE)
for (csv in table_files) {
  base <- file_path_sans_ext(basename(csv))
  latex_out <- file.path(latex_dir, paste0(base, ".tex"))
  csv_to_latex(csv, latex_out, caption = base)
}

cat("\nConversion CSV -> LaTeX terminée.\n")
