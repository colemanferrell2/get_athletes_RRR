library(readxl)
library(dplyr)
library(lubridate)
library(stringr)

process_world_table <- function(file_path, gender, event_type) {
  full_data <- NULL

  # Read the Excel sheet
  table_data <- read_excel(file_path, sheet = "Table 1")

  for (x in 0:27) {
    begin <- x * 51 + 2
    end <- x * 51 + 52
    chunk <- table_data[begin:end, 1:8]

    # Clean column names and remove extra rows/columns
    colnames(chunk) <- as.character(unlist(chunk[1, ]))
    chunk <- chunk[-1, ]
    chunk <- chunk[, -2]  # Drop the second column

    # Append to full dataset
    full_data <- bind_rows(full_data, chunk)
  }

  # Add gender and event type info
  full_data <- full_data %>%
    mutate(Gender = gender, EventType = event_type)

  return(full_data)
}

# Load and process all 4 files
men_middle <- process_world_table("/users/c/a/caferr/RRR/World Athletics/Pages from World Athletics Scoring Tables of Athlet.pdf.xlsx", "Men", "Middle")
men_long   <- process_world_table("/users/c/a/caferr/RRR/World Athletics/PointMenLD.xlsx", "Men", "Long")
women_middle <- process_world_table("/users/c/a/caferr/RRR/World Athletics/WMD.xlsx", "Women", "Middle")
women_long   <- process_world_table("/users/c/a/caferr/RRR/World Athletics/WLD.xlsx", "Women", "Long")

# Combine datasets by gender
men_combined <- bind_rows(men_middle, men_long)
women_combined <- bind_rows(women_middle, women_long)

# Convert time columns to numeric seconds
time_cols <- c("600m", "800m", "1000m", "1500m", "Mile", "2000m",
               "2000mSC", "3000m", "3000mSC", "2 Miles", "5000m", "10,000m")

for (col in time_cols) {
  if (col %in% names(men_combined)) {
    men_combined[[col]] <- as.numeric(ms(as.character(men_combined[[col]])))
  }
  if (col %in% names(women_combined)) {
    women_combined[[col]] <- as.numeric(ms(as.character(women_combined[[col]])))
  }
}

# Convert Points column to numeric
men_combined$Points <- as.numeric(as.character(men_combined$Points))
women_combined$Points <- as.numeric(as.character(women_combined$Points))

# Save to separate CSV files
write.csv(men_combined, "/users/c/a/caferr/RRR/World Athletics/Men_WA_Table.csv", row.names = FALSE)
write.csv(women_combined, "/users/c/a/caferr/RRR/World Athletics/Women_WA_Table.csv", row.names = FALSE)

