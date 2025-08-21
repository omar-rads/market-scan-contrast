# ---- We begin with the list of packages ----
library(here)
library(arrow)
library(dplyr)
library(lubridate)
library(stringr)
library(purrr)
library(tidyverse)


# ---- 0) Where 2022 lives (local) ----
ms22_dir <- here("data","raw", "22MS")  # Points to path where local 22MS lives under project root
files22  <- list.files(ms22_dir, pattern="\\.csv$", full.names = TRUE) # Should be list of csv files in 22MS folder

# ---- 1) Keep only the columns we’ll use everywhere (lossless, standardized) ----
varkeep <- c(
  # IDs / linking
  "ENROLID","MSCLMID","FACHDID","FACPROF",
  # dates
  "SVCDATE","TSVCDAT","YEAR","PDDATE",
  # procedures / classification
  "PROC1","PROCTYP","PROCMOD","REVCODE","SVCSCAT","QTY","UNITS",
  # diagnoses
  "DX1","DX2","DX3","DX4","DXVER",
  # costs
  "NETPAY","PAY","COPAY","COINS","DEDUCT",
  # demographics / geography
  "AGE","AGEGRP","SEX","REGION","MSA",
  # nice-to-have context
  "STDPLAC","PLANTYP","NTWKPROV","PROVID","NPI","MDC","DATATYP","MEDADV"
)

# ---- 2) Bronze writer: stream -> light-clean -> write partitioned Parquet (LOCAL) ----
bronze_dir <- here("parquet", "bronze") # Builds an absolute path under my project root
dir.create(bronze_dir, recursive = TRUE, showWarnings = FALSE) # Makes folder called parquet within the project folder

ingest_to_bronze <- function(path_csv) {
  message("Streaming: ", basename(path_csv))
  
  # Stream the CSV; keep only needed cols (saves memory)
  tab <- read_csv_arrow(path_csv, col_select = varkeep, as_data_frame = FALSE)
  
  # Light, lossless transforms (no heavy filtering):
  tab2 <- tab %>%
    mutate(
      # derive from SERVICE DATE (source of truth)
      svc_year = year(SVCDATE),
      svc_qtr  = quarter(SVCDATE),
      # robust price field
      plan_paid   = coalesce(NETPAY, PAY, 0),
      oop         = coalesce(COPAY,0) + coalesce(COINS,0) + coalesce(DEDUCT,0),
      allowed_est = coalesce(ALLOWED, plan_paid + oop),
      # handy ED flag (you can filter on it later)
      is_ed = (POS == 23) | str_starts(coalesce(REVCODE, ""), "045")
    )
  
  write_dataset(
    tab2,
    path = bronze_dir,
    format = "parquet",
    partitioning = c("svc_year","svc_qtr"),
    existing_data_behavior = "overwrite_or_ignore"
  )
  invisible(TRUE)
}

walk(files22, ingest_to_bronze)

# Sanity check: what partitions were created?
list.files(bronze_dir, recursive = TRUE)[1:20]

# ---- If the raw/MarketScan link isn't there but the drive is mounted, create it ----
ensure_marketscan_link <- function() {
  link_root  <- here("raw","MarketScan")
  drive_root <- "/Volumes/MagicStick/MarketScan"
  if (!dir.exists(link_root) && dir.exists(drive_root)) {
    dir.create(here("raw"), showWarnings = FALSE)
    ok <- file.symlink(from = drive_root, to = link_root)
    if (!ok) warning("Couldn't create symlink ", link_root, " -> ", drive_root)
  }
  invisible(link_root)
}

check_marketscan_link <- function() {
  link_root  <- here::here("raw","MarketScan")
  target     <- "/Volumes/MagicStick/MarketScan"
  
  # Does a symlink entry exist?
  link_entry_exists <- file.exists(link_root)
  
  # Is the target reachable? (dir.exists follows symlinks)
  target_reachable  <- dir.exists(link_root)
  
  if (!link_entry_exists) {
    message("Symlink missing: ", link_root, 
            "\nRun: ln -s ", target, " ", link_root)
  } else if (!target_reachable) {
    warning("Symlink exists but target not reachable.\n",
            "Is the drive mounted/unlocked at ", target, 
            " (or did it mount as 'MagicStick 1')?")
  } else {
    message("OK: ", link_root, " → ", normalizePath(link_root))
  }
}


ensure_marketscan_link()   # create if missing and drive is present
check_marketscan_link()    # warn if dangling/not mounted

p23 <- here::here("raw","MarketScan","23MS")
files23 <- list.files(p23, pattern="\\.csv$", full.names=TRUE)



#Here is the list of the variables I am keeping for the iodine study
#Keeping less than original helps with keeping the csv size small

varkeep <- c(
  "ENROLID","MSCLMID","FACHDID","FACPROF",
  "SVCDATE","YEAR",
  "POS","REVCODE","SVCSCAT",
  "PROCTYP","PROC1","PROCMOD",
  "DX1","DX2","DX3","DX4",
  "AGE","AGEGRP","SEX","REGION","MSA",
  "NETPAY","PAY","COPAY","COINS","DEDUCT","ALLOWED"
)

#Here are the CPT codes - of note this is only for CT scans with and without
#contrast ----
cpt_codes <- c(
  70450, 70460, 70470, 70480, 70481, 70482, 70486, 70487, 70488,
  70490, 70491, 70492, 70496, 70498, 71250, 71260, 71270, 71271, 71275,
  72125, 72126, 72127, 72128, 72129, 72130, 72131, 72132, 72133, 72191,
  72192, 72193, 72184, 73200, 73201, 73202, 73206, 73700, 73701, 73702,
  73706, 74150, 74160, 74170, 74174, 74175, 74176, 74177, 74178, 74261,
  74262, 74263, 75571, 75572, 75573, 75574, 76380, 76497
)

col_spec <- cols(
  ENROLID = col_character(),   # <-- make it character everywhere
  SEQNUM  = col_double(),
  DX1     = col_character(),
  PROC1   = col_double(),
  SVCDATE = col_date(format = "%Y%m%d"),  # adjust if your dates are m/d/Y etc.
  YEAR    = col_integer(),
  AGE     = col_double(),
  COB     = col_character(),
  REGION  = col_character(),
  MSA     = col_character(),
  AGEGRP  = col_character(),
  SEX     = col_character(),
  MSCLMID = col_character(),
  .default = col_skip()        # skip anything not named in varkeep
)

# Points to both year folders
data_dirs <- c(
  "/Users/omarjean-bap./Documents/Marketscan/MS23",
  "/Users/omarjean-bap./Documents/Marketscan/MS24"
)

csv_files <- map(data_dirs, list.files,
                 pattern = "\\.csv$", full.names = TRUE) %>%
  unlist(use.names = FALSE)

# 3. read, filter, stack
all_filtered <- csv_files %>%
  set_names() %>%                       # keeps source filename in .id
  map_dfr(
    ~ read_csv(.x, col_types = col_spec, show_col_types = FALSE) %>%
      select(all_of(varkeep)) %>%     
      filter(
        PROC1 %in% cpt_codes,         # your CT CPT list
        YEAR  %in% c(2023, 2024)      # <- optional safety check
      ),
    .id = "source_file"
  )

#####
#This give the path of all the files
csv_files <- list.files(
  path       = "/Users/omarjean-bap./Documents/Marketscan/MS24",
  pattern    = "\\.csv$",
  full.names = TRUE
)

#This is a very efficient way of combining the csv files in one folder
#to form a filtered data set in one step
all_filtered <- csv_files %>%
  set_names() %>%
  map_dfr(
    ~ read_csv(.x, col_types = col_spec, show_col_types = FALSE) %>%
      select(all_of(varkeep)) %>%          # keep only varkeep columns
      filter(PROC1 %in% cpt_codes),
    .id = "source_file"
  )

