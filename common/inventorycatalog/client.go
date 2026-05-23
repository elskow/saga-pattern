package inventorycatalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"saga-pattern/common/dto"
)

type CatalogItem struct {
	ProductID string      `json:"productId"`
	Name      string      `json:"name"`
	Price     json.Number `json:"price"`
	Available int         `json:"available"`
}

type InsufficientAvailabilityError struct {
	ProductID string
	Requested int
	Available int
}

func (e InsufficientAvailabilityError) Error() string {
	return fmt.Sprintf("insufficient available stock for product %s: requested %d, available %d", e.ProductID, e.Requested, e.Available)
}

func IsInsufficientAvailability(err error) bool {
	var target InsufficientAvailabilityError
	return errors.As(err, &target)
}

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string, httpClient *http.Client) (*Client, error) {
	trimmed := strings.TrimSpace(baseURL)
	if trimmed == "" {
		return nil, fmt.Errorf("inventory service url is required")
	}
	if _, err := url.Parse(trimmed); err != nil {
		return nil, fmt.Errorf("parse inventory service url: %w", err)
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 3 * time.Second}
	}
	clone := *httpClient
	transport := clone.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	clone.Transport = otelhttp.NewTransport(transport)
	httpClient = &clone
	return &Client{baseURL: strings.TrimRight(trimmed, "/"), httpClient: httpClient}, nil
}

func (c *Client) NormalizeOrderItems(ctx context.Context, items []dto.OrderItemRequest) ([]dto.OrderItemRequest, json.Number, error) {
	if len(items) == 0 {
		return nil, "", fmt.Errorf("items are required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.catalogURL(items), nil)
	if err != nil {
		return nil, "", fmt.Errorf("build inventory catalog request: %w", err)
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("inventory catalog lookup failed: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return nil, "", fmt.Errorf("inventory catalog lookup failed with status %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	var catalogItems []CatalogItem
	if err := json.NewDecoder(res.Body).Decode(&catalogItems); err != nil {
		return nil, "", fmt.Errorf("decode inventory catalog response: %w", err)
	}
	byID := make(map[string]CatalogItem, len(catalogItems))
	for _, item := range catalogItems {
		byID[item.ProductID] = item
	}
	requestedByID := make(map[string]int, len(items))
	for _, item := range items {
		requestedByID[item.ProductID] += item.Quantity
	}
	normalized := make([]dto.OrderItemRequest, 0, len(items))
	total := new(big.Rat)
	for _, item := range items {
		catalogItem, ok := byID[item.ProductID]
		if !ok {
			return nil, "", fmt.Errorf("inventory catalog product %s not found", item.ProductID)
		}
		if requestedByID[item.ProductID] > catalogItem.Available {
			return nil, "", InsufficientAvailabilityError{ProductID: item.ProductID, Requested: requestedByID[item.ProductID], Available: catalogItem.Available}
		}
		normalized = append(normalized, dto.OrderItemRequest{
			ProductID:   item.ProductID,
			ProductName: catalogItem.Name,
			Quantity:    item.Quantity,
			Price:       catalogItem.Price,
		})
		price := new(big.Rat)
		if _, ok := price.SetString(catalogItem.Price.String()); !ok {
			return nil, "", fmt.Errorf("parse inventory catalog price %q", catalogItem.Price.String())
		}
		lineTotal := new(big.Rat).Mul(price, big.NewRat(int64(item.Quantity), 1))
		total.Add(total, lineTotal)
	}
	return normalized, json.Number(total.FloatString(2)), nil
}

func (c *Client) catalogURL(items []dto.OrderItemRequest) string {
	query := url.Values{}
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if _, ok := seen[item.ProductID]; ok {
			continue
		}
		seen[item.ProductID] = struct{}{}
		query.Add("productId", item.ProductID)
	}
	return c.baseURL + "/api/catalog?" + query.Encode()
}
