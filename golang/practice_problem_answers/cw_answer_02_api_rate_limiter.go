// =============================================================================
// INTERVIEW PROBLEM 2: Tiered API Rate Limiter
// Difficulty: Senior Software Engineer | Estimated time: 40 min
// =============================================================================
//
// CONTEXT
// -------
// You're building the rate-limiting layer for a developer-facing API platform
// (think: Stripe, Vercel, Twilio — any product where external developers call
// your API and pay for usage tiers).
//
// Each API key belongs to a plan with per-minute and per-day request caps.
// Rate limiting uses a sliding window: a request at time T is within the
// per-minute cap if fewer than RPM requests occurred in the window (T-60, T],
// and within the per-day cap if fewer than RPD requests occurred in (T-86400, T].
//
// All timestamps are Unix time (float64 seconds).
//
// DATA MODEL
// ----------
// Plan       — RPM *int, RPD *int  (nil = unlimited)
// APIKey     — ID, Owner, Plan string; Enabled bool; RequestLog []float64
// Gateway    — Plans map[string]*Plan, Keys map[string]*APIKey
// RequestResult — Allowed bool, KeyID string, Reason string
// UsageStats    — KeyID, Plan string; RPMUsed, RPMLimit *int; RPDUsed, RPDLimit *int
//
// DEFAULT PLANS (used by MakeGateway when plans is nil)
// ------------------------------------------------------
//   "free":       RPM=60,    RPD=1_000
//   "starter":    RPM=300,   RPD=25_000
//   "pro":        RPM=1_000, RPD=200_000
//   "enterprise": RPM=nil,   RPD=nil  (unlimited)
//
// EXAMPLE
// -------
//   gw := MakeGateway(nil)
//   CreateKey(gw, "key_abc", "alice", "pro")
//   result := HandleRequest(gw, "key_abc", 1700000000.0)
//   // result.Allowed == true
//   stats, _ := GetUsage(gw, "key_abc", 1700000000.0)
//   // stats.RPMUsed == 1
// =============================================================================

package ratelimiter

import "errors"

// ErrAlreadyExists is returned when creating a resource with a duplicate ID.
var ErrAlreadyExists = errors.New("already exists")

// ErrNotFound is returned when accessing a resource that does not exist.
var ErrNotFound = errors.New("not found")

// ErrInvalidPlan is returned when referencing a plan name not in Gateway.Plans.
var ErrInvalidPlan = errors.New("invalid plan")

// IntPtr is a convenience helper for creating *int literals in plan definitions.
func IntPtr(i int) *int { return &i }

// Plan holds per-minute and per-day request caps. nil means unlimited.
type Plan struct {
	RPM *int // requests per minute; nil = unlimited
	RPD *int // requests per day;    nil = unlimited
}

// APIKey represents a registered API key.
type APIKey struct {
	ID         string
	Owner      string
	Plan       string // must be a key in Gateway.Plans
	Enabled    bool
	RequestLog []float64 // sorted Unix timestamps of recent requests
}

// Gateway holds all gateway state.
type Gateway struct {
	Plans map[string]*Plan
	Keys  map[string]*APIKey
}

// RequestResult is returned by HandleRequest.
type RequestResult struct {
	Allowed bool
	KeyID   string // set when Allowed == true
	Reason  string // set when Allowed == false: "key_not_found" | "key_disabled" | "rpm_exceeded" | "rpd_exceeded"
}

// UsageStats is returned by GetUsage.
type UsageStats struct {
	KeyID    string
	Plan     string
	RPMUsed  int
	RPMLimit *int // nil if unlimited
	RPDUsed  int
	RPDLimit *int // nil if unlimited
}

// DefaultPlans is used by MakeGateway when no custom plans are provided.
var DefaultPlans = map[string]*Plan{
	"free":       {RPM: IntPtr(60), RPD: IntPtr(1_000)},
	"starter":    {RPM: IntPtr(300), RPD: IntPtr(25_000)},
	"pro":        {RPM: IntPtr(1_000), RPD: IntPtr(200_000)},
	"enterprise": {RPM: nil, RPD: nil},
}

// MakeGateway returns a fresh Gateway state.
// If plans is nil, DefaultPlans is used.
func MakeGateway(plans map[string]*Plan) *Gateway {
	if plans == nil {
		return &Gateway{
			Plans: DefaultPlans,
			Keys:  make(map[string]*APIKey),
		}
	}

	return &Gateway{
		Plans: plans,
		Keys:  make(map[string]*APIKey),
	}
}

// ---------------------------------------------------------------------------
// PART 1 — Key management  (~10 min)
// ---------------------------------------------------------------------------

// CreateKey registers a new API key and returns it.
// Returns ErrAlreadyExists if keyID already exists.
// Returns ErrInvalidPlan if plan is not in gw.Plans.
func CreateKey(gw *Gateway, keyID, owner, plan string) (*APIKey, error) {
	if _, ok := gw.Plans[plan]; !ok {
		return nil, ErrInvalidPlan
	}

	if _, ok := gw.Keys[keyID]; ok {
		return nil, ErrAlreadyExists
	}

	gw.Keys[keyID] = &APIKey{
		ID:      keyID,
		Owner:   owner,
		Plan:    plan,
		Enabled: true,
	}

	return gw.Keys[keyID], nil
}

// RevokeKey sets key.Enabled = false.
// Keys are disabled rather than deleted so historical logs are preserved.
// Returns ErrNotFound if keyID is not found.
func RevokeKey(gw *Gateway, keyID string) error {
	if key, ok := gw.Keys[keyID]; ok {
		key.Enabled = false
		return nil
	}
	return ErrNotFound
}

// UpdatePlan changes a key's plan.
// The RequestLog is preserved (no reset on plan change).
// Returns ErrNotFound if keyID is not found.
// Returns ErrInvalidPlan if newPlan is not in gw.Plans.
func UpdatePlan(gw *Gateway, keyID, newPlan string) error {
	if _, ok := gw.Plans[newPlan]; !ok {
		return ErrInvalidPlan
	}

	if _, ok := gw.Keys[keyID]; !ok {
		return ErrNotFound
	}

	gw.Keys[keyID].Plan = newPlan
	return nil
}

// ---------------------------------------------------------------------------
// PART 2 — Sliding-window rate check  (~15 min)
// ---------------------------------------------------------------------------

// countInWindow returns the number of entries in requestLog that fall within
// the half-open window (now - windowSeconds, now].
// Helper — feel free to call this from IsAllowed and HandleRequest, or inline it.
func countInWindow(requestLog []float64, now, windowSeconds float64) int {
	minTs := now - windowSeconds
	cnt := 0
	for i := len(requestLog) - 1; i >= 0; i-- {
		if requestLog[i] <= minTs {
			break
		}
		cnt += 1
	}
	return cnt
}

// IsAllowed returns true if the key is allowed to make a request at time now.
//
// A request is NOT allowed if any of the following:
//   - keyID doesn't exist in gw.Keys
//   - key.Enabled is false
//   - the per-minute sliding window count >= plan's RPM cap
//   - the per-day sliding window count >= plan's RPD cap
//
// A nil limit means that dimension is unlimited.
func IsAllowed(gw *Gateway, keyID string, now float64) bool {
	apiKey, ok := gw.Keys[keyID]
	if !ok || !apiKey.Enabled {
		return false
	}

	rpm, rpd := gw.Plans[apiKey.Plan].RPM, gw.Plans[apiKey.Plan].RPD

	if (rpm == nil || countInWindow(apiKey.RequestLog, now, 60) < *rpm) &&
		(rpd == nil || countInWindow(apiKey.RequestLog, now, 86400) < *rpd) {
		return true
	}

	return false
}

// ---------------------------------------------------------------------------
// PART 3 — Recording requests + pruning  (~5 min)
// ---------------------------------------------------------------------------

// RecordRequest appends now to key.RequestLog and prunes any entries older
// than 25 hours (90_000 seconds) to bound memory usage.
// Call this only AFTER confirming the request is allowed.
// Returns ErrNotFound if keyID is not found.
func RecordRequest(gw *Gateway, keyID string, now float64) error {
	if _, ok := gw.Keys[keyID]; !ok {
		return ErrNotFound
	}

	for i := len(gw.Keys[keyID].RequestLog) - 1; i >= 0; i-- {
		if gw.Keys[keyID].RequestLog[i] <= now-90000 {
			gw.Keys[keyID].RequestLog = gw.Keys[keyID].RequestLog[i+1:]
			break
		}
	}
	gw.Keys[keyID].RequestLog = append(gw.Keys[keyID].RequestLog, now)
	return nil
}

// ---------------------------------------------------------------------------
// PART 4 — Combined handler + usage stats  (~10 min)
// ---------------------------------------------------------------------------

// HandleRequest attempts to process a request.
//
// On success (request allowed):
//
//	&RequestResult{Allowed: true, KeyID: keyID}
//	Side effect: records the request via RecordRequest.
//
// On failure:
//
//	&RequestResult{Allowed: false, Reason: <one of the strings below>}
//	No side effects.
//
// Reason strings:
//
//	"key_not_found" — keyID not in gw.Keys
//	"key_disabled"  — key exists but Enabled == false
//	"rpm_exceeded"  — per-minute cap reached
//	"rpd_exceeded"  — per-day cap reached (only when rpm is within limit)
func HandleRequest(gw *Gateway, keyID string, now float64) *RequestResult {
	key, ok := gw.Keys[keyID]
	if !ok {
		return &RequestResult{
			Allowed: false,
			KeyID:   keyID,
			Reason:  "key_not_found",
		}
	}

	if !key.Enabled {
		return &RequestResult{
			Allowed: false,
			KeyID:   keyID,
			Reason:  "key_disabled",
		}
	}

	rpmPlan, rpdPlan := gw.Plans[key.Plan].RPM, gw.Plans[key.Plan].RPD
	rpm, rpd := countInWindow(key.RequestLog, now, 60), countInWindow(key.RequestLog, now, 86400)

	if rpmPlan != nil && rpm >= *rpmPlan {
		return &RequestResult{
			Allowed: false,
			KeyID:   keyID,
			Reason:  "rpm_exceeded",
		}
	}

	if rpdPlan != nil && rpd >= *rpdPlan {
		return &RequestResult{
			Allowed: false,
			KeyID:   keyID,
			Reason:  "rpd_exceeded",
		}
	}

	_ = RecordRequest(gw, keyID, now)
	return &RequestResult{
		Allowed: true,
		KeyID:   keyID,
	}
}

// GetUsage returns current usage stats for a key.
// Returns ErrNotFound if keyID is not found.
func GetUsage(gw *Gateway, keyID string, now float64) (*UsageStats, error) {
	key, ok := gw.Keys[keyID]
	if !ok {
		return nil, ErrNotFound
	}

	rpmPlan, rpdPlan := gw.Plans[key.Plan].RPM, gw.Plans[key.Plan].RPD
	rpm, rpd := countInWindow(key.RequestLog, now, 60), countInWindow(key.RequestLog, now, 86400)

	return &UsageStats{
		KeyID:    keyID,
		Plan:     key.Plan,
		RPMUsed:  rpm,
		RPMLimit: rpmPlan,
		RPDUsed:  rpd,
		RPDLimit: rpdPlan,
	}, nil
}
