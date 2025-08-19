library(readr)
library(tidyverse)

dataset1 <- read_csv("/Users/omarjeanbaptiste/Data/radiology/market-scan-contrast/raw/ccaeo240_0_0_0.csv")

#Here is the list of the variables I am keeping for the iodine study
#Keeping less than original helps with keeping the csv size small
varkeep <- c("ENROLID", "SEQNUM", "DX1", "PROC1", "SVCDATE", "YEAR",
             "AGE", "COB", "REGION", "MSA", "AGEGRP", 
             "SEX", "MSCLMID")
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

