#!/usr/bin/env Rscript

#' Installation des packages R requis pour l'analyse économétrique
Sys.setenv(PATH = "/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/opt/homebrew/bin")
cat("🔧 Installing required R packages...\n")

# List of required packages
required_packages <- c(
  "dplyr",
  "readr", 
  "stringr",
  "broom",
  "fixest",      # Pour estimations panel efficaces
  "modelsummary", # Pour tables publication
  "xtable",      # Tables LaTeX
  "logger"       # Logging détaillé
)

# Function to install packages if not already installed
install_if_missing <- function(packages) {
  for (pkg in packages) {
    if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
      cat("📦 Installing package:", pkg, "\n")
      install.packages(pkg, repos = "https://cran.rstudio.com/")
      
      # Verify installation
      if (require(pkg, character.only = TRUE, quietly = TRUE)) {
        cat("✅ Successfully installed:", pkg, "\n")
      } else {
        cat("❌ Failed to install:", pkg, "\n")
      }
    } else {
      cat("✅ Package already installed:", pkg, "\n")
    }
  }
}

# Install packages
install_if_missing(required_packages)

cat("🎉 R package installation completed!\n")
