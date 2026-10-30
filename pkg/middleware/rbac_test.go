package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/xerr"
)

func TestPermissionsContext(t *testing.T) {
	ctx := context.Background()
	if perms := FromPermissions(ctx); perms != nil {
		t.Fatalf("expected nil permissions on empty context, got %v", perms)
	}

	ctx = WithPermissions(ctx, []string{" user:add ", "user:edit", ""})
	got := FromPermissions(ctx)
	if len(got) != 2 || got[0] != "user:add" || got[1] != "user:edit" {
		t.Fatalf("unexpected permissions from context: %v", got)
	}
}

func TestRequireRoles(t *testing.T) {
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		response.OkJson(r.Context(), w, "allowed")
	}

	mw := RequireRoles("editor", "auditor")(okHandler)

	// 1. 未登录（无 Claims）拦截 -> 403
	t.Run("No Claims Forbidden", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/doc", nil)
		rec := httptest.NewRecorder()
		mw(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected HTTP 403, got %d", rec.Code)
		}
		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Code != xerr.Forbidden {
			t.Fatalf("expected xerr.Forbidden (%d), got %d", xerr.Forbidden, resp.Code)
		}
	})

	// 2. 角色不匹配拦截 -> 403
	t.Run("Role Mismatch Forbidden", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/doc", nil)
		ctx := WithClaims(req.Context(), &jwt.BladeClaims{
			UserID:   10,
			RoleName: "guest,viewer",
		})
		rec := httptest.NewRecorder()
		mw(rec, req.WithContext(ctx))

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected HTTP 403 for guest,viewer, got %d", rec.Code)
		}
	})

	// 3. 逗号分隔的多角色命中其中一个允许角色 -> 200 放行
	t.Run("Matching Role In Comma Separated List Allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/doc", nil)
		ctx := WithClaims(req.Context(), &jwt.BladeClaims{
			UserID:   11,
			RoleName: "user, auditor",
		})
		rec := httptest.NewRecorder()
		mw(rec, req.WithContext(ctx))

		if rec.Code != http.StatusOK {
			t.Fatalf("expected HTTP 200 for auditor, got %d", rec.Code)
		}
	})

	// 4. 超级管理员角色 administrator / admin 直接放行 -> 200
	t.Run("SuperAdmin Role Allowed", func(t *testing.T) {
		for _, superRole := range []string{"administrator", "admin", "user,administrator"} {
			req := httptest.NewRequest(http.MethodGet, "/api/doc", nil)
			ctx := WithClaims(req.Context(), &jwt.BladeClaims{
				UserID:   1,
				RoleName: superRole,
			})
			rec := httptest.NewRecorder()
			mw(rec, req.WithContext(ctx))

			if rec.Code != http.StatusOK {
				t.Fatalf("expected HTTP 200 for super role %q, got %d", superRole, rec.Code)
			}
		}
	})
}

func TestRequirePermissions(t *testing.T) {
	okHandler := func(w http.ResponseWriter, r *http.Request) {
		response.OkJson(r.Context(), w, "perm_ok")
	}

	mw := RequirePermissions("system:user:add", "system:user:edit")(okHandler)

	// 1. 无权限码且非超管 -> 403
	t.Run("Missing Permissions Forbidden", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/user", nil)
		ctx := WithClaims(req.Context(), &jwt.BladeClaims{
			UserID:   20,
			RoleName: "editor",
		})
		ctx = WithPermissions(ctx, []string{"system:user:add"}) // 缺少 system:user:edit
		rec := httptest.NewRecorder()
		mw(rec, req.WithContext(ctx))

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected HTTP 403 when missing one required permission, got %d", rec.Code)
		}
		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Code != xerr.Forbidden {
			t.Fatalf("expected xerr.Forbidden, got %d", resp.Code)
		}
	})

	// 2. 具备全部所需权限码 -> 200 放行
	t.Run("All Required Permissions Present Allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/user", nil)
		ctx := WithClaims(req.Context(), &jwt.BladeClaims{
			UserID:   21,
			RoleName: "editor",
		})
		ctx = WithPermissions(ctx, []string{"system:user:view", "system:user:add", "system:user:edit"})
		rec := httptest.NewRecorder()
		mw(rec, req.WithContext(ctx))

		if rec.Code != http.StatusOK {
			t.Fatalf("expected HTTP 200 when all permissions satisfied, got %d", rec.Code)
		}
	})

	// 3. 超级管理员角色 administrator 免权限码校验直接放行 -> 200
	t.Run("Administrator Bypasses Permission Check", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/user", nil)
		ctx := WithClaims(req.Context(), &jwt.BladeClaims{
			UserID:   1,
			RoleName: "administrator",
		})
		rec := httptest.NewRecorder()
		mw(rec, req.WithContext(ctx))

		if rec.Code != http.StatusOK {
			t.Fatalf("expected HTTP 200 for administrator without explicit permissions, got %d", rec.Code)
		}
	})
}
