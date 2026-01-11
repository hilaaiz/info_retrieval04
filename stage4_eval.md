## Stage 4 – Evolution Evaluation

How did the Prime Minister’s rhetoric regarding the war between Israel and Hamas/Gaza change between his first and last speech?

### Query 1
Query

Has the official position in the last quarter of 2023 changed relative to the official position in the last quarter of 2025?

Baseline Retrieval (Standard RAG)

Year range: 2023 → 2025

Observation: Retrieved chunks span multiple years and mix unrelated content.

Example evidence:

2023: “Mr. Speaker, today, I introduce the District of Columbia National Guard Commanding General Residency Act…”

2025: “She is survived by her children, Paul, Bill, and Laura…”

Issue:
The baseline retriever does not respect temporal constraints and combines documents from different periods without distinction.

Temporal Retrieval (Stage 4)
Early Period

Year range: 2023 → 2023

Evidence: All retrieved chunks originate from 2023 only.

Late Period

Year range: 2025 → 2025

Evidence: All retrieved chunks originate from 2025 only.

Evaluation

The temporal retrieval mechanism successfully separates documents from different time periods, preventing temporal mixing observed in the baseline retrieval. While the retrieved content is not always semantically aligned with the query topic, the experiment demonstrates that the system enforces strict temporal boundaries, which is a prerequisite for reliable evolutionary reasoning. This highlights that temporal awareness alone is insufficient without strong semantic retrieval, an important limitation revealed by the evaluation.


## Query
Who is the Minister of Defense / Secretary of Defense?

---

## Baseline Retrieval (Standard RAG)

**Year range:** 2023 → 2024

**Observed behavior:**  
The baseline retriever returns documents from multiple years without enforcing temporal constraints. The retrieved chunks are loosely related to defense topics (e.g., mentions of the Secretary of Defense) but also include unrelated honors and commemorative texts.

**Example evidence:**
- **2023:** “The Jackson Lee Amendment No. 1099 requires the Secretary of Defense to report…”
- **2024:** “HONORING PAUL ELMSTRAND, MATTHEW RUGE, AND ADAM FINSETH…”

**Issue:**  
The baseline system does not distinguish between office holders across time and risks ambiguity when roles change between years.

---

## Temporal Retrieval (Stage 4 – Evolution)

### Early Period
**Year range:** 2023 → 2023

All retrieved chunks originate exclusively from 2023. References to the Secretary of Defense appear only in documents from this period, with no contamination from later years.

### Late Period
**Year range:** 2025 → 2025

All retrieved chunks originate exclusively from 2025. Documents are strictly separated from the early period, preventing temporal overlap.

---

## Evaluation

This example demonstrates that the temporal retrieval mechanism successfully enforces strict separation between early and late periods. While the retrieved content does not always directly identify the office holder by name, the system prevents temporal mixing that could otherwise cause ambiguity in role-based questions. Compared to the baseline retrieval, which spans multiple years simultaneously, Stage 4 provides a cleaner temporal context that is necessary for resolving conflicts when official roles change over time.

---

## Why This Example Is Successful

- Baseline retrieval mixes documents from different years.
- Temporal retrieval cleanly separates early (2023) and late (2025) evidence.
- The system avoids temporal ambiguity in a role-based (conflict) query.
- This validates the temporal awareness of the Stage 4 pipeline, even when semantic relevance is imperfect.

