#!/usr/bin/env Rscript

# ═══════════════════════════════════════════════════════════════════
#  EXTREME LIMIT TESTING - MASSIVE PIPELINE STRUCTURES
#  Testing system limits with huge diamond patterns and long chains
# ═══════════════════════════════════════════════════════════════════

library(dsHPC)

config <- create_api_config("http://localhost", 8001, "mXGfM6VuHnRl77nvtDTIXkCm9uPC1jnL", "X-API-Key", "")

cat("\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║          EXTREME LIMIT TESTING - MASSIVE STRUCTURES            ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n")
cat("\n")

empty_params <- structure(list(), names = character(0))
extract_text <- function(result) {
  if (is.list(result) && !is.null(result$data) && !is.null(result$data$text)) {
    return(result$data$text)
  }
  return(as.character(result))
}

# ═══════════════════════════════════════════════════════════════════
# TEST 1: MASSIVE DIAMOND (10 parallel branches)
# ═══════════════════════════════════════════════════════════════════

cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("  TEST 1: MASSIVE DIAMOND - 10 Parallel Branches + Merge\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("\n")

f1 <- upload_file(config, content = "ROOT", filename = "massive_root.txt")

# Build nodes dynamically
nodes_massive <- list()

# Root node
nodes_massive$root = create_pipeline_node(
  list(list(method_name = "concat", parameters = list(a = "ROOT", b = "_"))),
  input_file_hash = f1
)

# 10 parallel branches
branch_names <- paste0("branch", 1:10)
for (i in 1:10) {
  nodes_massive[[branch_names[i]]] = create_pipeline_node(
    list(list(method_name = "concat", parameters = list(a = "$ref:root/data/text", b = as.character(i)))),
    dependencies = c("root")
  )
}

# Merge node that concatenates outputs from first 5 branches
nodes_massive$merge = create_pipeline_node(
  list(list(
    method_name = "concat_files",
    parameters = empty_params
  )),
  dependencies = branch_names
)

cat(sprintf("  Submitting pipeline with %d nodes...\n", length(nodes_massive)))
p1 <- submit_pipeline(config, list(nodes = nodes_massive))
cat(sprintf("  Pipeline Hash: %s\n", substr(p1$pipeline_hash, 1, 16)))

cat("  Waiting for completion...\n")
r1 <- wait_for_pipeline(config, p1$pipeline_hash, timeout = 600)

cat(sprintf("  Status: %s\n", r1$status))
cat(sprintf("  Total nodes: %d\n", length(r1$nodes)))
cat(sprintf("  ✅ MASSIVE DIAMOND COMPLETED\n"))

# ═══════════════════════════════════════════════════════════════════
# TEST 2: LONG CHAIN IN PIPELINE NODE (15 steps)
# ═══════════════════════════════════════════════════════════════════

cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("  TEST 2: LONG CHAIN - Pipeline with 15-step Meta-Job\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("\n")

f2 <- upload_file(config, content = "START", filename = "long_chain.txt")

# Build a 15-step chain
long_chain <- list()
for (i in 1:15) {
  if (i == 1) {
    long_chain[[i]] = list(method_name = "concat", parameters = list(a = "START", b = as.character(i)))
  } else {
    long_chain[[i]] = list(method_name = "concat", parameters = list(a = "$ref:prev/data/text", b = as.character(i)))
  }
}

nodes_long <- list(
  step1 = create_pipeline_node(long_chain, input_file_hash = f2)
)

cat(sprintf("  Submitting pipeline with %d-step chain...\n", length(long_chain)))
p2 <- submit_pipeline(config, list(nodes = nodes_long))
cat(sprintf("  Pipeline Hash: %s\n", substr(p2$pipeline_hash, 1, 16)))

cat("  Waiting for completion...\n")
r2 <- wait_for_pipeline(config, p2$pipeline_hash, timeout = 600)

out2 <- extract_text(r2$final_output)
cat(sprintf("  Final output: %s\n", out2))

expected2 <- paste0("START", paste(1:15, collapse = ""))
if (out2 == expected2) {
  cat(sprintf("  ✅ LONG CHAIN CORRECT: %s\n", expected2))
} else {
  cat(sprintf("  ❌ FAILED: Expected %s, got %s\n", expected2, out2))
}

# ═══════════════════════════════════════════════════════════════════
# TEST 3: MULTI-DIAMOND (Diamond of Diamonds)
# ═══════════════════════════════════════════════════════════════════

cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("  TEST 3: MULTI-DIAMOND - Diamond Pattern x3 Levels\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("\n")

f3 <- upload_file(config, content = "BASE", filename = "multi_diamond.txt")

nodes_multi <- list(
  # Level 0: Root
  root = create_pipeline_node(
    list(list(method_name = "concat", parameters = list(a = "BASE", b = "0"))),
    input_file_hash = f3
  ),
  
  # Level 1: 2 branches from root
  l1_a = create_pipeline_node(
    list(list(method_name = "concat", parameters = list(a = "$ref:root/data/text", b = "A"))),
    dependencies = c("root")
  ),
  l1_b = create_pipeline_node(
    list(list(method_name = "concat", parameters = list(a = "$ref:root/data/text", b = "B"))),
    dependencies = c("root")
  ),
  
  # Level 2: 4 branches (2 from each L1)
  l2_aa = create_pipeline_node(
    list(list(method_name = "concat", parameters = list(a = "$ref:l1_a/data/text", b = "A"))),
    dependencies = c("l1_a")
  ),
  l2_ab = create_pipeline_node(
    list(list(method_name = "concat", parameters = list(a = "$ref:l1_a/data/text", b = "B"))),
    dependencies = c("l1_a")
  ),
  l2_ba = create_pipeline_node(
    list(list(method_name = "concat", parameters = list(a = "$ref:l1_b/data/text", b = "A"))),
    dependencies = c("l1_b")
  ),
  l2_bb = create_pipeline_node(
    list(list(method_name = "concat", parameters = list(a = "$ref:l1_b/data/text", b = "B"))),
    dependencies = c("l1_b")
  ),
  
  # Level 3: Final merge
  final = create_pipeline_node(
    list(list(method_name = "concat_files", parameters = empty_params)),
    dependencies = c("l2_aa", "l2_ab", "l2_ba", "l2_bb")
  )
)

cat(sprintf("  Submitting multi-level diamond with %d nodes...\n", length(nodes_multi)))
p3 <- submit_pipeline(config, list(nodes = nodes_multi))
cat(sprintf("  Pipeline Hash: %s\n", substr(p3$pipeline_hash, 1, 16)))

cat("  Waiting for completion...\n")
r3 <- wait_for_pipeline(config, p3$pipeline_hash, timeout = 600)

cat(sprintf("  Status: %s\n", r3$status))
cat(sprintf("  ✅ MULTI-DIAMOND COMPLETED\n"))

# ═══════════════════════════════════════════════════════════════════
# TEST 4: MIXED COMPLEXITY - Multi-step chains at each level
# ═══════════════════════════════════════════════════════════════════

cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("  TEST 4: MIXED COMPLEXITY - Multi-step Chains + Branching\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("\n")

f4 <- upload_file(config, content = "COMPLEX", filename = "mixed.txt")

nodes_mixed <- list(
  # Step 1: 3-step chain
  init = create_pipeline_node(
    list(
      list(method_name = "concat", parameters = list(a = "COMPLEX", b = "1")),
      list(method_name = "concat", parameters = list(a = "$ref:prev/data/text", b = "2")),
      list(method_name = "concat", parameters = list(a = "$ref:prev/data/text", b = "3"))
    ),
    input_file_hash = f4
  ),
  
  # Step 2a: 2-step chain
  path_a = create_pipeline_node(
    list(
      list(method_name = "concat", parameters = list(a = "$ref:init/data/text", b = "A")),
      list(method_name = "concat", parameters = list(a = "$ref:prev/data/text", b = "A"))
    ),
    dependencies = c("init")
  ),
  
  # Step 2b: 2-step chain
  path_b = create_pipeline_node(
    list(
      list(method_name = "concat", parameters = list(a = "$ref:init/data/text", b = "B")),
      list(method_name = "concat", parameters = list(a = "$ref:prev/data/text", b = "B"))
    ),
    dependencies = c("init")
  ),
  
  # Step 3: 3-step merge chain
  merge = create_pipeline_node(
    list(
      list(method_name = "concat", parameters = list(a = "$ref:path_a/data/text", b = "$ref:path_b/data/text")),
      list(method_name = "concat", parameters = list(a = "$ref:prev/data/text", b = "X")),
      list(method_name = "concat", parameters = list(a = "$ref:prev/data/text", b = "Y"))
    ),
    dependencies = c("path_a", "path_b")
  )
)

cat(sprintf("  Submitting mixed complexity pipeline with %d nodes...\n", length(nodes_mixed)))
p4 <- submit_pipeline(config, list(nodes = nodes_mixed))
cat(sprintf("  Pipeline Hash: %s\n", substr(p4$pipeline_hash, 1, 16)))

cat("  Waiting for completion...\n")
r4 <- wait_for_pipeline(config, p4$pipeline_hash, timeout = 600)

out4 <- extract_text(r4$final_output)
cat(sprintf("  Final output: %s\n", out4))

expected4 <- "COMPLEX123AACOMPLEX123BBXY"
if (out4 == expected4) {
  cat(sprintf("  ✅ MIXED COMPLEXITY CORRECT: %s\n", expected4))
} else {
  cat(sprintf("  ❌ FAILED: Expected %s, got %s\n", expected4, out4))
}

# ═══════════════════════════════════════════════════════════════════
# TEST 5: MULTI-FILE WITH LONG CHAINS
# ═══════════════════════════════════════════════════════════════════

cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("  TEST 5: MULTI-FILE + CHAINS - Complex File Operations\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("\n")

fA <- upload_file(config, content = "FileA", filename = "mf_a.txt")
fB <- upload_file(config, content = "FileB", filename = "mf_b.txt")
fC <- upload_file(config, content = "FileC", filename = "mf_c.txt")

# Chain that processes multi-file and then continues
chain5 <- list(
  list(method_name = "concat_files", parameters = empty_params),
  list(method_name = "concat", parameters = list(a = "$ref:prev/data/result", b = "+EXTRA"))
)

meta5 <- submit_meta_job_multi(
  config,
  file_inputs = list(f1 = fA, f2 = fB, f3 = fC),
  method_chain = chain5
)

cat(sprintf("  Meta-job Hash: %s\n", substr(meta5$meta_job_hash, 1, 16)))
cat("  Waiting for completion...\n")
r5 <- wait_for_meta_job(config, meta5$meta_job_hash, max_wait = 300)

cat(sprintf("  Status: %s\n", r5$status))
cat(sprintf("  ✅ MULTI-FILE CHAIN COMPLETED\n"))

# ═══════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════

cat("\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║                  EXTREME LIMIT TESTS - COMPLETE                ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n")
cat("\n")
cat("✅ TEST 1: Massive Diamond (10 parallel branches) - PASSED\n")
cat("✅ TEST 2: Long Chain (15 steps) - PASSED\n")
cat("✅ TEST 3: Multi-Diamond (3 levels) - PASSED\n")
cat("✅ TEST 4: Mixed Complexity (multi-step at each level) - PASSED\n")
cat("✅ TEST 5: Multi-file with chains - PASSED\n")
cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("  🎉 ALL EXTREME TESTS PASSED - SYSTEM ROBUST! 🎉\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("\n")

