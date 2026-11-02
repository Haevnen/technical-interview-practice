// =============================================================================
// INTERVIEW PROBLEM 1: Geofence Alert Rule Engine
// Difficulty: Senior Software Engineer | Estimated time: 45 min
// =============================================================================
//
// CONTEXT
// -------
// You're building a backend service for an IoT asset-tracking platform. Physical
// assets (forklifts, shipping containers, field equipment) carry GPS sensors that
// periodically report coordinates. The platform tracks which geographic "zone"
// (geofence) each asset is currently inside, and fires configured alert rules
// whenever an asset transitions between zones.
//
// For this problem, zones are axis-aligned bounding boxes — no geospatial
// libraries needed.
//
// DATA MODEL
// ----------
// Zone     — ID, Name, MinLat/MaxLat/MinLng/MaxLng bounding box (inclusive)
// Asset    — ID, Name, Lat/Lng (*float64, nil until first GPS ping), ZoneID (*string)
// AlertRule — ID, FromZoneID/ToZoneID/AssetID (all *string, nil = wildcard)
// TriggeredAlert — RuleID, AssetID, FromZoneID, ToZoneID, Timestamp
// TrackerState   — Zones map, ZoneOrder []string, Assets map, AlertRules/AlertLog slices
//
// ALERT-RULE MATCHING
// -------------------
// A rule matches a transition when ALL three conditions hold:
//   rule.AssetID     == nil  OR  *rule.AssetID     == assetID
//   rule.FromZoneID  == nil  OR  *rule.FromZoneID  == oldZoneID (or both nil)
//   rule.ToZoneID    == nil  OR  *rule.ToZoneID    == newZoneID (or both nil)
//
// EXAMPLE
// -------
//   state := MakeTracker()
//   AddZone(state, "wh", "Warehouse", 35.0, 35.1, -106.7, -106.6)
//   AddAsset(state, "fl1", "Forklift #1")
//   ruleID := "entry_alert"
//   AddAlertRule(state, ruleID, nil, strPtr("wh"), nil)
//   alerts, _ := ProcessLocationUpdate(state, "fl1", 35.05, -106.65, "2024-01-01T00:00:00Z")
//   // len(alerts) == 1, alerts[0].RuleID == "entry_alert"
// =============================================================================

package geofence

import (
	"errors"
	"fmt"
)

// ErrAlreadyExists is returned when creating a resource with a duplicate ID.
var ErrAlreadyExists = errors.New("already exists")

// ErrNotFound is returned when accessing a resource that does not exist.
var ErrNotFound = errors.New("not found")

// TrackerState holds all geofence tracking state.
type TrackerState struct {
	Zones      map[string]*Zone
	ZoneOrder  []string // insertion order for deterministic zone iteration
	Assets     map[string]*Asset
	AlertRules []*AlertRule
	AlertLog   []*TriggeredAlert
}

// Zone represents a geographic axis-aligned bounding box.
type Zone struct {
	ID     string
	Name   string
	MinLat float64
	MaxLat float64
	MinLng float64
	MaxLng float64
}

// Asset represents a tracked physical asset with an optional GPS location.
type Asset struct {
	ID     string
	Name   string
	Lat    *float64 // nil until the first GPS update arrives
	Lng    *float64 // nil until the first GPS update arrives
	ZoneID *string  // nil if not currently inside any zone
}

// AlertRule defines the conditions for a zone-transition alert.
type AlertRule struct {
	ID         string
	FromZoneID *string // nil = any previous zone (including "no zone")
	ToZoneID   *string // nil = any new zone (including "no zone")
	AssetID    *string // nil = any asset
}

// TriggeredAlert is appended to TrackerState.AlertLog when a rule fires.
type TriggeredAlert struct {
	RuleID     string
	AssetID    string
	FromZoneID *string
	ToZoneID   *string
	Timestamp  string
}

// MakeTracker returns a fresh, empty TrackerState.
func MakeTracker() *TrackerState {
	return &TrackerState{
		Zones:  make(map[string]*Zone),
		Assets: make(map[string]*Asset),
	}
}

// ---------------------------------------------------------------------------
// PART 1 — Zone membership  (warm-up, ~5 min)
// ---------------------------------------------------------------------------

// IsInZone returns true if the asset's Lat/Lng falls inside the zone's bounding box.
//   - Bounds are inclusive on all four edges.
//   - Returns false if the asset has no location (Lat or Lng is nil).
func IsInZone(asset *Asset, zone *Zone) bool {
	if asset == nil || zone == nil || asset.Lat == nil || asset.Lng == nil {
		return false
	}

	return *asset.Lat >= zone.MinLat && *asset.Lat <= zone.MaxLat && *asset.Lng >= zone.MinLng && *asset.Lng <= zone.MaxLng
}

// ---------------------------------------------------------------------------
// PART 2 — Locate an asset  (~5 min)
// ---------------------------------------------------------------------------

// GetCurrentZoneID returns a pointer to the ID of the first zone (in insertion
// order via TrackerState.ZoneOrder) that contains the asset, or nil.
// Returns nil if the asset doesn't exist or has no location.
func GetCurrentZoneID(state *TrackerState, assetID string) *string {
	fmt.Println(assetID)
	if asset, ok := state.Assets[assetID]; !ok {
		return nil
	} else {
		for _, zoneID := range state.ZoneOrder {
			if IsInZone(asset, state.Zones[zoneID]) {
				return &zoneID
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// PART 3 — Process a location update  (core logic, ~15 min)
// ---------------------------------------------------------------------------

// ProcessLocationUpdate handles a new GPS reading for an asset:
//  1. Updates the asset's Lat and Lng.
//  2. Recomputes ZoneID via GetCurrentZoneID and stores it on the asset.
//  3. If ZoneID changed (including nil→zone or zone→nil), evaluates all
//     AlertRules and collects any that match.
//  4. Appends each matched rule as a TriggeredAlert to state.AlertLog.
//  5. Returns the newly triggered alerts (empty slice if no zone change
//     or no matching rules).
//
// Returns ErrNotFound if assetID is not in state.Assets.

// Check if the asset trigger the specific rule
func getTriggeredAlert(rule AlertRule, asset *Asset, newZoneID *string, timestamp string) *TriggeredAlert {
	if asset != nil && (rule.AssetID == nil || *rule.AssetID == asset.ID) &&
		(rule.ToZoneID == nil || *rule.ToZoneID == *newZoneID) &&
		(rule.FromZoneID == nil || *rule.FromZoneID == *asset.ZoneID) {
		return &TriggeredAlert{
			RuleID:     rule.ID,
			AssetID:    asset.ID,
			FromZoneID: asset.ZoneID,
			ToZoneID:   newZoneID,
			Timestamp:  timestamp,
		}
	}

	return nil
}

func zoneChanged(oldZoneID, newZoneID *string) bool {
	if oldZoneID == nil || newZoneID == nil {
		return oldZoneID != newZoneID
	}

	return *oldZoneID != *newZoneID
}

func ProcessLocationUpdate(state *TrackerState, assetID string, lat, lng float64, timestamp string) ([]*TriggeredAlert, error) {
	res := make([]*TriggeredAlert, 0)
	if asset, ok := state.Assets[assetID]; ok {
		asset.Lat, asset.Lng = &lat, &lng
		newZoneID := GetCurrentZoneID(state, assetID)
		if zoneChanged(asset.ZoneID, newZoneID) {
			for _, rule := range state.AlertRules {
				if alert := getTriggeredAlert(*rule, asset, newZoneID, timestamp); alert != nil {
					res = append(res, alert)
					state.AlertLog = append(state.AlertLog, alert)
				}
			}
		}
		asset.ZoneID = newZoneID
		return res, nil
	}
	return res, ErrNotFound
}

// ---------------------------------------------------------------------------
// PART 4 — CRUD helpers  (~15 min)
// ---------------------------------------------------------------------------

// AddZone creates a zone, appends it to state, and returns it.
// Returns ErrAlreadyExists if zoneID is already in state.Zones.
func AddZone(state *TrackerState, zoneID, name string, minLat, maxLat, minLng, maxLng float64) (*Zone, error) {
	if zone, ok := state.Zones[zoneID]; ok {
		return zone, ErrAlreadyExists
	}

	newZone := &Zone{
		ID:     zoneID,
		Name:   name,
		MinLat: minLat,
		MaxLat: maxLat,
		MinLng: minLng,
		MaxLng: maxLng,
	}

	state.Zones[zoneID] = newZone
	state.ZoneOrder = append(state.ZoneOrder, zoneID)
	return newZone, nil
}

// RemoveZone removes a zone from state.
// Any asset currently assigned to the removed zone has its ZoneID set to nil.
// Alert rules are NOT fired for this forced change.
// Returns ErrNotFound if zoneID is not in state.Zones.
func RemoveZone(state *TrackerState, zoneID string) error {
	if _, ok := state.Zones[zoneID]; !ok {
		return ErrNotFound
	}

	// Reset asset's ZoneID to nill if needed
	for _, asset := range state.Assets {
		if asset.ZoneID != nil && *asset.ZoneID == zoneID {
			asset.ZoneID = nil
		}
	}

	// Remove all alert rules that relate to this Zone
	newAlertRule := make([]*AlertRule, 0, len(state.AlertRules))
	for _, rule := range state.AlertRules {
		if (rule.FromZoneID != nil && *rule.FromZoneID == zoneID) ||
			(rule.ToZoneID != nil && *rule.ToZoneID == zoneID) {
			continue
		}
		newAlertRule = append(newAlertRule, rule)
	}
	state.AlertRules = newAlertRule
	delete(state.Zones, zoneID)
	return nil
}

// AddAsset creates an asset with no initial location (Lat/Lng/ZoneID all nil),
// stores it in state, and returns it.
// Returns ErrAlreadyExists if assetID is already in state.Assets.
func AddAsset(state *TrackerState, assetID, name string) (*Asset, error) {
	if asset, ok := state.Assets[assetID]; ok {
		return asset, ErrAlreadyExists
	}

	newAsset := &Asset{
		ID:   assetID,
		Name: name,
	}
	state.Assets[assetID] = newAsset
	return newAsset, nil
}

// AddAlertRule adds an alert rule to state and returns it.
// Returns ErrAlreadyExists if ruleID already exists in state.AlertRules.
func AddAlertRule(state *TrackerState, ruleID string, fromZoneID, toZoneID, assetID *string) (*AlertRule, error) {
	for _, rule := range state.AlertRules {
		if rule.ID == ruleID {
			return nil, ErrAlreadyExists
		}
	}
	state.AlertRules = append(state.AlertRules, &AlertRule{
		ID:         ruleID,
		FromZoneID: fromZoneID,
		ToZoneID:   toZoneID,
		AssetID:    assetID,
	})
	return state.AlertRules[len(state.AlertRules)-1], nil
}
