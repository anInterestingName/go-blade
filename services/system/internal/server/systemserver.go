// Package server 实现 SystemService gRPC 服务契约。
package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	systemv1 "github.com/go-blade/api/proto/system/v1"
	"github.com/go-blade/pkg/database"
	"github.com/go-blade/pkg/event"
	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
	"github.com/go-blade/services/system/internal/model"
	"github.com/go-blade/services/system/internal/svc"
	"github.com/zeromicro/go-zero/core/logx"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

// SystemServer 实现 systemv1.SystemServiceServer 接口契约。
type SystemServer struct {
	systemv1.UnimplementedSystemServiceServer
	svcCtx *svc.ServiceContext
}

// NewSystemServer 构造 SystemServer 实例。
func NewSystemServer(svcCtx *svc.ServiceContext) *SystemServer {
	return &SystemServer{
		svcCtx: svcCtx,
	}
}

// GetTenant 查询指定租户的配置与元数据。
func (s *SystemServer) GetTenant(ctx context.Context, req *systemv1.GetTenantRequest) (*systemv1.GetTenantResponse, error) {
	if req == nil || strings.TrimSpace(req.TenantId) == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tenant_id cannot be empty")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	var t model.Tenant
	err := s.svcCtx.DB.WithContext(ctx).Where("tenant_id = ? AND is_deleted = 0", strings.TrimSpace(req.TenantId)).First(&t).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "租户不存在")
		}
		logx.WithContext(ctx).Errorf("query tenant failed: %v", err)
		return nil, status.Error(codes.Internal, "query tenant failed")
	}

	return &systemv1.GetTenantResponse{
		Tenant: &systemv1.TenantInfo{
			Id:            t.ID.Int64(),
			TenantId:      t.TenantID,
			TenantName:    t.TenantName,
			Linkman:       t.Linkman,
			ContactNumber: t.ContactNumber,
			Address:       t.Address,
			Status:        int32(t.Status),
		},
	}, nil
}

// ListTenants 分页查询系统租户列表。
func (s *SystemServer) ListTenants(ctx context.Context, req *systemv1.ListTenantsRequest) (*systemv1.ListTenantsResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	current, size := normalizePage(req.GetCurrent(), req.GetSize())

	query := s.svcCtx.DB.WithContext(ctx).Model(&model.Tenant{}).Where("is_deleted = 0")
	if tid := strings.TrimSpace(req.GetTenantId()); tid != "" {
		query = query.Where("tenant_id LIKE ?", "%"+tid+"%")
	}
	if tname := strings.TrimSpace(req.GetTenantName()); tname != "" {
		query = query.Where("tenant_name LIKE ?", "%"+tname+"%")
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		logx.WithContext(ctx).Errorf("count tenants failed: %v", err)
		return nil, status.Error(codes.Internal, "count tenants failed")
	}

	var tenants []model.Tenant
	offset := int((current - 1) * size)
	if err := query.Order("id asc").Offset(offset).Limit(int(size)).Find(&tenants).Error; err != nil {
		logx.WithContext(ctx).Errorf("list tenants failed: %v", err)
		return nil, status.Error(codes.Internal, "list tenants failed")
	}

	records := make([]*systemv1.TenantInfo, 0, len(tenants))
	for _, t := range tenants {
		records = append(records, &systemv1.TenantInfo{
			Id:            t.ID.Int64(),
			TenantId:      t.TenantID,
			TenantName:    t.TenantName,
			Linkman:       t.Linkman,
			ContactNumber: t.ContactNumber,
			Address:       t.Address,
			Status:        int32(t.Status),
		})
	}

	return &systemv1.ListTenantsResponse{
		Records: records,
		Total:   total,
		Size:    size,
		Current: current,
	}, nil
}

// SaveTenant 新增或更新系统租户（未指定 tenant_id 时自动生成 6 位数字租户编号）。
func (s *SystemServer) SaveTenant(ctx context.Context, req *systemv1.SaveTenantRequest) (*systemv1.SaveTenantResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	tenantName := strings.TrimSpace(req.TenantName)
	if tenantName == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_name cannot be empty")
	}

	tenantStatus := int(req.Status)
	if tenantStatus <= 0 {
		tenantStatus = 1
	}

	if req.Id == 0 {
		newID := snowflake.NextID()
		tenantCode := strings.TrimSpace(req.TenantId)
		if tenantCode == "" {
			tenantCode = fmt.Sprintf("%06d", (newID.Int64()%900000)+100000)
		}
		t := model.Tenant{
			ID:            newID,
			TenantID:      tenantCode,
			TenantName:    tenantName,
			Linkman:       strings.TrimSpace(req.Linkman),
			ContactNumber: strings.TrimSpace(req.ContactNumber),
			Address:       strings.TrimSpace(req.Address),
			Status:        tenantStatus,
		}
		if err := s.svcCtx.DB.WithContext(ctx).Create(&t).Error; err != nil {
			logx.WithContext(ctx).Errorf("create tenant failed: %v", err)
			return nil, status.Error(codes.Internal, "create tenant failed")
		}
		return &systemv1.SaveTenantResponse{
			Id:       t.ID.Int64(),
			TenantId: t.TenantID,
		}, nil
	}

	var existing model.Tenant
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.Id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "租户不存在")
		}
		return nil, status.Error(codes.Internal, "query tenant failed")
	}

	updates := map[string]any{
		"tenant_name":    tenantName,
		"linkman":        strings.TrimSpace(req.Linkman),
		"contact_number": strings.TrimSpace(req.ContactNumber),
		"address":        strings.TrimSpace(req.Address),
		"status":         tenantStatus,
	}
	if err := s.svcCtx.DB.WithContext(ctx).Model(&model.Tenant{}).Where("id = ? AND is_deleted = 0", req.Id).Updates(updates).Error; err != nil {
		logx.WithContext(ctx).Errorf("update tenant failed: %v", err)
		return nil, status.Error(codes.Internal, "update tenant failed")
	}

	return &systemv1.SaveTenantResponse{
		Id:       existing.ID.Int64(),
		TenantId: existing.TenantID,
	}, nil
}

// DeleteTenants 批量逻辑删除系统租户（强制保护顶级管理租户 "000000" 不可被删除）。
func (s *SystemServer) DeleteTenants(ctx context.Context, req *systemv1.DeleteTenantsRequest) (*systemv1.DeleteTenantsResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil || len(req.Ids) == 0 {
		return &systemv1.DeleteTenantsResponse{Affected: 0}, nil
	}

	res := s.svcCtx.DB.WithContext(ctx).
		Model(&model.Tenant{}).
		Where("id IN ? AND tenant_id <> ? AND is_deleted = 0", req.Ids, tenant.DefaultTenantID).
		Update("is_deleted", 1)
	if res.Error != nil {
		logx.WithContext(ctx).Errorf("delete tenants failed: %v", res.Error)
		return nil, status.Error(codes.Internal, "delete tenants failed")
	}

	return &systemv1.DeleteTenantsResponse{Affected: res.RowsAffected}, nil
}

// GetUser 查询用户详情及关联的权限信息。
func (s *SystemServer) GetUser(ctx context.Context, req *systemv1.GetUserRequest) (*systemv1.GetUserResponse, error) {
	if req == nil || req.UserId <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "user_id must be greater than 0")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	var user model.User
	err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.UserId).First(&user).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "用户不存在")
		}
		logx.WithContext(ctx).Errorf("query user failed: %v", err)
		return nil, status.Error(codes.Internal, "query user failed")
	}

	if user.Status != 1 {
		return nil, status.Error(codes.PermissionDenied, "账号已被冻结")
	}

	roleIDs := parseRoleIDList(user.RoleID)
	var roles []string
	var permissions []string

	if len(roleIDs) > 0 {
		var roleModels []model.Role
		if err := s.svcCtx.DB.WithContext(ctx).Where("id IN ? AND is_deleted = 0", roleIDs).Order("sort asc, id asc").Find(&roleModels).Error; err != nil {
			logx.WithContext(ctx).Errorf("query roles failed: %v", err)
			return nil, status.Error(codes.Internal, "query roles failed")
		}
		validRoleIDs := make([]int64, 0, len(roleModels))
		for _, r := range roleModels {
			validRoleIDs = append(validRoleIDs, r.ID.Int64())
			if alias := strings.TrimSpace(r.RoleAlias); alias != "" {
				roles = append(roles, alias)
			}
		}

		if len(validRoleIDs) > 0 {
			var roleMenus []model.RoleMenu
			if err := s.svcCtx.DB.WithContext(ctx).Where("role_id IN ?", validRoleIDs).Find(&roleMenus).Error; err != nil {
				logx.WithContext(ctx).Errorf("query role_menu failed: %v", err)
				return nil, status.Error(codes.Internal, "query role menus failed")
			}

			menuIDs := extractMenuIDs(roleMenus)
			if len(menuIDs) > 0 {
				var btnMenus []model.Menu
				if err := s.svcCtx.DB.WithContext(ctx).Where("id IN ? AND category = 2 AND is_deleted = 0", menuIDs).Order("sort asc, id asc").Find(&btnMenus).Error; err != nil {
					logx.WithContext(ctx).Errorf("query button permissions failed: %v", err)
					return nil, status.Error(codes.Internal, "query permissions failed")
				}
				seen := make(map[string]struct{}, len(btnMenus))
				for _, m := range btnMenus {
					act := strings.TrimSpace(m.Action)
					if act != "" {
						if _, exists := seen[act]; !exists {
							seen[act] = struct{}{}
							permissions = append(permissions, act)
						}
					}
				}
			}
		}
	}

	return &systemv1.GetUserResponse{
		User:        toUserInfo(&user),
		Roles:       roles,
		Permissions: permissions,
	}, nil
}

// GetUserRoutes 获取当前用户的菜单树与按钮权限列表。
func (s *SystemServer) GetUserRoutes(ctx context.Context, req *systemv1.GetUserRoutesRequest) (*systemv1.GetUserRoutesResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil {
		return &systemv1.GetUserRoutesResponse{}, nil
	}

	roleIDStr := ""
	if req.UserId > 0 {
		var user model.User
		if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.UserId).First(&user).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return &systemv1.GetUserRoutesResponse{}, nil
			}
			logx.WithContext(ctx).Errorf("query user for routes failed: %v", err)
			return nil, status.Error(codes.Internal, "query user routes failed")
		}
		if user.Status != 1 {
			return nil, status.Error(codes.PermissionDenied, "账号已被冻结")
		}
		roleIDStr = user.RoleID
	} else {
		roleIDStr = strings.TrimSpace(req.RoleIds)
	}

	roleIDs := parseRoleIDList(roleIDStr)
	if len(roleIDs) == 0 {
		return &systemv1.GetUserRoutesResponse{}, nil
	}

	// 先过滤出当前租户下未删除的有效角色 ID，确保多租户角色边界严密
	var validRoles []model.Role
	if err := s.svcCtx.DB.WithContext(ctx).Where("id IN ? AND is_deleted = 0", roleIDs).Find(&validRoles).Error; err != nil {
		logx.WithContext(ctx).Errorf("query valid roles for routes failed: %v", err)
		return nil, status.Error(codes.Internal, "query valid roles failed")
	}
	validRoleIDs := make([]int64, 0, len(validRoles))
	for _, r := range validRoles {
		validRoleIDs = append(validRoleIDs, r.ID.Int64())
	}
	if len(validRoleIDs) == 0 {
		return &systemv1.GetUserRoutesResponse{}, nil
	}

	var roleMenus []model.RoleMenu
	if err := s.svcCtx.DB.WithContext(ctx).Where("role_id IN ?", validRoleIDs).Find(&roleMenus).Error; err != nil {
		logx.WithContext(ctx).Errorf("query role_menu for routes failed: %v", err)
		return nil, status.Error(codes.Internal, "query role menus failed")
	}

	menuIDs := extractMenuIDs(roleMenus)
	if len(menuIDs) == 0 {
		return &systemv1.GetUserRoutesResponse{}, nil
	}

	var menus []model.Menu
	if err := s.svcCtx.DB.WithContext(ctx).Where("id IN ? AND is_deleted = 0", menuIDs).Order("sort asc, id asc").Find(&menus).Error; err != nil {
		logx.WithContext(ctx).Errorf("query menus failed: %v", err)
		return nil, status.Error(codes.Internal, "query menus failed")
	}

	var protoRoutes []*systemv1.MenuInfo
	var buttons []string
	seenBtn := make(map[string]struct{})
	for _, m := range menus {
		if m.Category == 1 {
			protoRoutes = append(protoRoutes, toMenuInfo(&m))
		} else if m.Category == 2 {
			act := strings.TrimSpace(m.Action)
			if act != "" {
				if _, exists := seenBtn[act]; !exists {
					seenBtn[act] = struct{}{}
					buttons = append(buttons, act)
				}
			}
		}
	}

	return &systemv1.GetUserRoutesResponse{
		Routes:  protoRoutes,
		Buttons: buttons,
	}, nil
}

// ListUsers 分页查询系统用户列表。
func (s *SystemServer) ListUsers(ctx context.Context, req *systemv1.ListUsersRequest) (*systemv1.ListUsersResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	current, size := normalizePage(req.GetCurrent(), req.GetSize())

	scopedCtx := s.resolveUserDataScope(ctx)
	query := s.svcCtx.DB.WithContext(scopedCtx).Model(&model.User{}).Where("is_deleted = 0")
	if acc := strings.TrimSpace(req.GetAccount()); acc != "" {
		query = query.Where("account LIKE ?", "%"+acc+"%")
	}
	if rn := strings.TrimSpace(req.GetRealName()); rn != "" {
		query = query.Where("real_name LIKE ?", "%"+rn+"%")
	}
	if req.GetDeptId() > 0 {
		query = query.Where("dept_id = ?", req.GetDeptId())
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		logx.WithContext(ctx).Errorf("count users failed: %v", err)
		return nil, status.Error(codes.Internal, "count users failed")
	}

	var users []model.User
	offset := int((current - 1) * size)
	if err := query.Order("id desc").Offset(offset).Limit(int(size)).Find(&users).Error; err != nil {
		logx.WithContext(ctx).Errorf("list users failed: %v", err)
		return nil, status.Error(codes.Internal, "list users failed")
	}

	records := make([]*systemv1.UserInfo, 0, len(users))
	for i := range users {
		records = append(records, toUserInfo(&users[i]))
	}

	return &systemv1.ListUsersResponse{
		Records: records,
		Total:   total,
		Size:    size,
		Current: current,
	}, nil
}

// SaveUser 新增或更新系统用户。
func (s *SystemServer) SaveUser(ctx context.Context, req *systemv1.SaveUserRequest) (*systemv1.SaveUserResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	if req.Id == 0 {
		account := strings.TrimSpace(req.Account)
		if account == "" {
			return nil, status.Error(codes.InvalidArgument, "account cannot be empty")
		}
		var dupCount int64
		if err := s.svcCtx.DB.WithContext(ctx).Model(&model.User{}).Where("account = ? AND is_deleted = 0", account).Count(&dupCount).Error; err != nil {
			logx.WithContext(ctx).Errorf("check duplicate user account failed: %v", err)
			return nil, status.Error(codes.Internal, "check user account failed")
		}
		if dupCount > 0 {
			return nil, xerr.ToGrpcError(xerr.NewCodeError(xerr.DataAlreadyExist, "该登录账号已存在，请勿重复创建"))
		}
		rawPwd := strings.TrimSpace(req.Password)
		if rawPwd == "" {
			rawPwd = "123456"
		}
		hashed, err := bcrypt.GenerateFromPassword([]byte(rawPwd), bcrypt.DefaultCost)
		if err != nil {
			logx.WithContext(ctx).Errorf("bcrypt password failed: %v", err)
			return nil, status.Error(codes.Internal, "hash password failed")
		}

		userStatus := int(req.Status)
		if userStatus == 0 {
			userStatus = 1
		}

		user := model.User{
			ID:       snowflake.ID(snowflake.GenID()),
			Account:  account,
			Password: string(hashed),
			Name:     strings.TrimSpace(req.Name),
			RealName: strings.TrimSpace(req.RealName),
			Email:    strings.TrimSpace(req.Email),
			Phone:    strings.TrimSpace(req.Phone),
			DeptID:   snowflake.ID(req.DeptId),
			RoleID:   strings.TrimSpace(req.RoleId),
			Status:   userStatus,
		}
		if err := s.svcCtx.DB.WithContext(ctx).Create(&user).Error; err != nil {
			logx.WithContext(ctx).Errorf("create user failed: %v", err)
			return nil, status.Error(codes.Internal, "create user failed")
		}
		s.publishRBACChanged(ctx, "save_user", nil, []int64{user.ID.Int64()})
		return &systemv1.SaveUserResponse{Id: user.ID.Int64()}, nil
	}

	var existing model.User
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.Id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "用户不存在")
		}
		logx.WithContext(ctx).Errorf("query user for update failed: %v", err)
		return nil, status.Error(codes.Internal, "query user failed")
	}

	updates := map[string]any{
		"name":      strings.TrimSpace(req.Name),
		"real_name": strings.TrimSpace(req.RealName),
		"email":     strings.TrimSpace(req.Email),
		"phone":     strings.TrimSpace(req.Phone),
		"dept_id":   snowflake.ID(req.DeptId),
		"role_id":   strings.TrimSpace(req.RoleId),
	}
	if acc := strings.TrimSpace(req.Account); acc != "" && acc != existing.Account {
		var dupCount int64
		if err := s.svcCtx.DB.WithContext(ctx).Model(&model.User{}).Where("account = ? AND id <> ? AND is_deleted = 0", acc, req.Id).Count(&dupCount).Error; err != nil {
			logx.WithContext(ctx).Errorf("check duplicate user account on update failed: %v", err)
			return nil, status.Error(codes.Internal, "check user account failed")
		}
		if dupCount > 0 {
			return nil, xerr.ToGrpcError(xerr.NewCodeError(xerr.DataAlreadyExist, "该登录账号已存在，请勿重复使用"))
		}
		updates["account"] = acc
	}
	if req.Status > 0 {
		updates["status"] = int(req.Status)
	}
	if rawPwd := strings.TrimSpace(req.Password); rawPwd != "" {
		hashed, err := bcrypt.GenerateFromPassword([]byte(rawPwd), bcrypt.DefaultCost)
		if err != nil {
			logx.WithContext(ctx).Errorf("bcrypt password failed: %v", err)
			return nil, status.Error(codes.Internal, "hash password failed")
		}
		updates["password"] = string(hashed)
	}

	if err := s.svcCtx.DB.WithContext(ctx).Model(&model.User{}).Where("id = ? AND is_deleted = 0", req.Id).Updates(updates).Error; err != nil {
		logx.WithContext(ctx).Errorf("update user failed: %v", err)
		return nil, status.Error(codes.Internal, "update user failed")
	}

	s.publishRBACChanged(ctx, "save_user", nil, []int64{req.Id})
	return &systemv1.SaveUserResponse{Id: req.Id}, nil
}

// DeleteUsers 批量逻辑删除系统用户。
func (s *SystemServer) DeleteUsers(ctx context.Context, req *systemv1.DeleteUsersRequest) (*systemv1.DeleteUsersResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil || len(req.Ids) == 0 {
		return &systemv1.DeleteUsersResponse{Affected: 0}, nil
	}

	res := s.svcCtx.DB.WithContext(ctx).Model(&model.User{}).Where("id IN ? AND is_deleted = 0", req.Ids).Update("is_deleted", 1)
	if res.Error != nil {
		logx.WithContext(ctx).Errorf("delete users failed: %v", res.Error)
		return nil, status.Error(codes.Internal, "delete users failed")
	}

	if res.RowsAffected > 0 {
		s.publishRBACChanged(ctx, "delete_users", nil, req.Ids)
	}
	return &systemv1.DeleteUsersResponse{Affected: res.RowsAffected}, nil
}

// GetDeptTree 获取系统部门树形结构。
func (s *SystemServer) GetDeptTree(ctx context.Context, req *systemv1.GetDeptTreeRequest) (*systemv1.GetDeptTreeResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	query := s.svcCtx.DB.WithContext(ctx).Model(&model.Dept{}).Where("is_deleted = 0")
	if name := strings.TrimSpace(req.GetDeptName()); name != "" {
		query = query.Where("dept_name LIKE ?", "%"+name+"%")
	}

	var depts []model.Dept
	if err := query.Order("sort asc, id asc").Find(&depts).Error; err != nil {
		logx.WithContext(ctx).Errorf("query depts failed: %v", err)
		return nil, status.Error(codes.Internal, "query depts failed")
	}

	return &systemv1.GetDeptTreeResponse{
		Tree: buildDeptTree(depts),
	}, nil
}

// SaveDept 新增或更新部门信息。
func (s *SystemServer) SaveDept(ctx context.Context, req *systemv1.SaveDeptRequest) (*systemv1.SaveDeptResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	if req.Id == 0 {
		deptName := strings.TrimSpace(req.DeptName)
		if deptName == "" {
			return nil, status.Error(codes.InvalidArgument, "dept_name cannot be empty")
		}
		fullName := strings.TrimSpace(req.FullName)
		if fullName == "" {
			fullName = deptName
		}
		dept := model.Dept{
			ID:       snowflake.ID(snowflake.GenID()),
			ParentID: snowflake.ID(req.ParentId),
			DeptName: deptName,
			FullName: fullName,
			Sort:     int(req.Sort),
			Remark:   strings.TrimSpace(req.Remark),
		}
		if err := s.svcCtx.DB.WithContext(ctx).Create(&dept).Error; err != nil {
			logx.WithContext(ctx).Errorf("create dept failed: %v", err)
			return nil, status.Error(codes.Internal, "create dept failed")
		}
		return &systemv1.SaveDeptResponse{Id: dept.ID.Int64()}, nil
	}

	var existing model.Dept
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.Id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "部门不存在")
		}
		logx.WithContext(ctx).Errorf("query dept for update failed: %v", err)
		return nil, status.Error(codes.Internal, "query dept failed")
	}

	updates := map[string]any{
		"parent_id": snowflake.ID(req.ParentId),
		"dept_name": strings.TrimSpace(req.DeptName),
		"full_name": strings.TrimSpace(req.FullName),
		"sort":      int(req.Sort),
		"remark":    strings.TrimSpace(req.Remark),
	}
	if err := s.svcCtx.DB.WithContext(ctx).Model(&model.Dept{}).Where("id = ? AND is_deleted = 0", req.Id).Updates(updates).Error; err != nil {
		logx.WithContext(ctx).Errorf("update dept failed: %v", err)
		return nil, status.Error(codes.Internal, "update dept failed")
	}

	return &systemv1.SaveDeptResponse{Id: req.Id}, nil
}

// DeleteDepts 批量逻辑删除部门。
func (s *SystemServer) DeleteDepts(ctx context.Context, req *systemv1.DeleteDeptsRequest) (*systemv1.DeleteDeptsResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil || len(req.Ids) == 0 {
		return &systemv1.DeleteDeptsResponse{Affected: 0}, nil
	}

	res := s.svcCtx.DB.WithContext(ctx).Model(&model.Dept{}).Where("id IN ? AND is_deleted = 0", req.Ids).Update("is_deleted", 1)
	if res.Error != nil {
		logx.WithContext(ctx).Errorf("delete depts failed: %v", res.Error)
		return nil, status.Error(codes.Internal, "delete depts failed")
	}

	return &systemv1.DeleteDeptsResponse{Affected: res.RowsAffected}, nil
}

// ListRoles 分页查询角色列表。
func (s *SystemServer) ListRoles(ctx context.Context, req *systemv1.ListRolesRequest) (*systemv1.ListRolesResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	current, size := normalizePage(req.GetCurrent(), req.GetSize())

	query := s.svcCtx.DB.WithContext(ctx).Model(&model.Role{}).Where("is_deleted = 0")
	if rn := strings.TrimSpace(req.GetRoleName()); rn != "" {
		query = query.Where("role_name LIKE ?", "%"+rn+"%")
	}
	if ra := strings.TrimSpace(req.GetRoleAlias()); ra != "" {
		query = query.Where("role_alias LIKE ?", "%"+ra+"%")
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		logx.WithContext(ctx).Errorf("count roles failed: %v", err)
		return nil, status.Error(codes.Internal, "count roles failed")
	}

	var roles []model.Role
	offset := int((current - 1) * size)
	if err := query.Order("sort asc, id asc").Offset(offset).Limit(int(size)).Find(&roles).Error; err != nil {
		logx.WithContext(ctx).Errorf("list roles failed: %v", err)
		return nil, status.Error(codes.Internal, "list roles failed")
	}

	records := make([]*systemv1.RoleInfo, 0, len(roles))
	for i := range roles {
		r := roles[i]
		records = append(records, &systemv1.RoleInfo{
			Id:               r.ID.Int64(),
			TenantId:         r.TenantID,
			ParentId:         r.ParentID.Int64(),
			RoleName:         r.RoleName,
			RoleAlias:        r.RoleAlias,
			Sort:             int32(r.Sort),
			DataScopeType:    int32(r.DataScopeType),
			DataScopeDeptIds: r.DataScopeDeptIDs,
		})
	}

	return &systemv1.ListRolesResponse{
		Records: records,
		Total:   total,
		Size:    size,
		Current: current,
	}, nil
}

// SaveRole 新增或更新角色。
func (s *SystemServer) SaveRole(ctx context.Context, req *systemv1.SaveRoleRequest) (*systemv1.SaveRoleResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	if req.Id == 0 {
		roleName := strings.TrimSpace(req.RoleName)
		if roleName == "" {
			return nil, status.Error(codes.InvalidArgument, "role_name cannot be empty")
		}
		dsType := int(req.DataScopeType)
		if dsType <= 0 {
			dsType = 1
		}
		role := model.Role{
			ID:               snowflake.ID(snowflake.GenID()),
			ParentID:         snowflake.ID(req.ParentId),
			RoleName:         roleName,
			RoleAlias:        strings.TrimSpace(req.RoleAlias),
			DataScopeType:    dsType,
			DataScopeDeptIDs: strings.TrimSpace(req.DataScopeDeptIds),
			Sort:             int(req.Sort),
		}
		if err := s.svcCtx.DB.WithContext(ctx).Create(&role).Error; err != nil {
			logx.WithContext(ctx).Errorf("create role failed: %v", err)
			return nil, status.Error(codes.Internal, "create role failed")
		}
		s.publishRBACChanged(ctx, "save_role", []int64{role.ID.Int64()}, nil)
		return &systemv1.SaveRoleResponse{Id: role.ID.Int64()}, nil
	}

	var existing model.Role
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.Id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "角色不存在")
		}
		logx.WithContext(ctx).Errorf("query role for update failed: %v", err)
		return nil, status.Error(codes.Internal, "query role failed")
	}

	updates := map[string]any{
		"parent_id":  snowflake.ID(req.ParentId),
		"role_name":  strings.TrimSpace(req.RoleName),
		"role_alias": strings.TrimSpace(req.RoleAlias),
		"sort":       int(req.Sort),
	}
	if req.DataScopeType > 0 {
		updates["data_scope_type"] = int(req.DataScopeType)
	}
	if req.DataScopeDeptIds != "" {
		updates["data_scope_dept_ids"] = strings.TrimSpace(req.DataScopeDeptIds)
	}
	if err := s.svcCtx.DB.WithContext(ctx).Model(&model.Role{}).Where("id = ? AND is_deleted = 0", req.Id).Updates(updates).Error; err != nil {
		logx.WithContext(ctx).Errorf("update role failed: %v", err)
		return nil, status.Error(codes.Internal, "update role failed")
	}

	s.publishRBACChanged(ctx, "save_role", []int64{req.Id}, nil)
	return &systemv1.SaveRoleResponse{Id: req.Id}, nil
}

// DeleteRoles 批量逻辑删除角色。
func (s *SystemServer) DeleteRoles(ctx context.Context, req *systemv1.DeleteRolesRequest) (*systemv1.DeleteRolesResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil || len(req.Ids) == 0 {
		return &systemv1.DeleteRolesResponse{Affected: 0}, nil
	}

	res := s.svcCtx.DB.WithContext(ctx).Model(&model.Role{}).Where("id IN ? AND is_deleted = 0", req.Ids).Update("is_deleted", 1)
	if res.Error != nil {
		logx.WithContext(ctx).Errorf("delete roles failed: %v", res.Error)
		return nil, status.Error(codes.Internal, "delete roles failed")
	}

	if res.RowsAffected > 0 {
		s.publishRBACChanged(ctx, "delete_roles", req.Ids, nil)
	}
	return &systemv1.DeleteRolesResponse{Affected: res.RowsAffected}, nil
}

// GrantRoleMenus 为角色分配菜单与按钮权限。
func (s *SystemServer) GrantRoleMenus(ctx context.Context, req *systemv1.GrantRoleMenusRequest) (*systemv1.GrantRoleMenusResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil || len(req.RoleIds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "role_ids cannot be empty")
	}

	// 校验当前租户下可授权的有效角色 ID
	var validRoles []model.Role
	if err := s.svcCtx.DB.WithContext(ctx).Where("id IN ? AND is_deleted = 0", req.RoleIds).Find(&validRoles).Error; err != nil {
		logx.WithContext(ctx).Errorf("query valid roles for grant failed: %v", err)
		return nil, status.Error(codes.Internal, "query roles failed")
	}
	validRoleIDs := make([]int64, 0, len(validRoles))
	for _, r := range validRoles {
		validRoleIDs = append(validRoleIDs, r.ID.Int64())
	}
	if len(validRoleIDs) == 0 {
		return &systemv1.GrantRoleMenusResponse{Success: false}, nil
	}

	err := s.svcCtx.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("role_id IN ?", validRoleIDs).Delete(&model.RoleMenu{}).Error; err != nil {
			return err
		}
		if len(req.MenuIds) == 0 {
			return nil
		}

		seenMenus := make(map[int64]struct{}, len(req.MenuIds))
		uniqueMenus := make([]int64, 0, len(req.MenuIds))
		for _, mid := range req.MenuIds {
			if mid > 0 {
				if _, ok := seenMenus[mid]; !ok {
					seenMenus[mid] = struct{}{}
					uniqueMenus = append(uniqueMenus, mid)
				}
			}
		}
		if len(uniqueMenus) == 0 {
			return nil
		}

		records := make([]model.RoleMenu, 0, len(validRoleIDs)*len(uniqueMenus))
		for _, rid := range validRoleIDs {
			for _, mid := range uniqueMenus {
				records = append(records, model.RoleMenu{
					ID:     snowflake.ID(snowflake.GenID()),
					RoleID: snowflake.ID(rid),
					MenuID: snowflake.ID(mid),
				})
			}
		}
		return tx.Create(&records).Error
	})
	if err != nil {
		logx.WithContext(ctx).Errorf("grant role menus failed: %v", err)
		return nil, status.Error(codes.Internal, "grant role menus failed")
	}

	s.publishRBACChanged(ctx, "grant_role_menus", validRoleIDs, nil)
	return &systemv1.GrantRoleMenusResponse{Success: true}, nil
}

// GrantRoleDataScope 为指定角色设置行级数据权限类型与自定义部门 ID 集合。
func (s *SystemServer) GrantRoleDataScope(ctx context.Context, req *systemv1.GrantRoleDataScopeRequest) (*systemv1.GrantRoleDataScopeResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil || req.RoleId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "role_id must be greater than 0")
	}

	var existing model.Role
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.RoleId).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "角色不存在")
		}
		return nil, status.Error(codes.Internal, "query role failed")
	}

	dsType := int(req.DataScopeType)
	if dsType <= 0 {
		dsType = int(database.DataScopeAll)
	}

	deptStrParts := make([]string, 0, len(req.DeptIds))
	seen := make(map[int64]struct{}, len(req.DeptIds))
	for _, did := range req.DeptIds {
		if did > 0 {
			if _, ok := seen[did]; !ok {
				seen[did] = struct{}{}
				deptStrParts = append(deptStrParts, strconv.FormatInt(did, 10))
			}
		}
	}

	updates := map[string]any{
		"data_scope_type":     dsType,
		"data_scope_dept_ids": strings.Join(deptStrParts, ","),
	}
	if err := s.svcCtx.DB.WithContext(ctx).Model(&model.Role{}).Where("id = ? AND is_deleted = 0", req.RoleId).Updates(updates).Error; err != nil {
		logx.WithContext(ctx).Errorf("grant role data scope failed: %v", err)
		return nil, status.Error(codes.Internal, "grant role data scope failed")
	}

	s.publishRBACChanged(ctx, "grant_role_datascope", []int64{req.RoleId}, nil)
	return &systemv1.GrantRoleDataScopeResponse{Success: true}, nil
}

// GetMenuTree 查询菜单与权限按钮列表。
func (s *SystemServer) GetMenuTree(ctx context.Context, req *systemv1.GetMenuTreeRequest) (*systemv1.GetMenuTreeResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	query := s.svcCtx.DB.WithContext(ctx).Model(&model.Menu{}).Where("is_deleted = 0")
	if name := strings.TrimSpace(req.GetName()); name != "" {
		query = query.Where("name LIKE ?", "%"+name+"%")
	}
	if req.GetCategory() > 0 {
		query = query.Where("category = ?", req.GetCategory())
	}

	var menus []model.Menu
	if err := query.Order("sort asc, id asc").Find(&menus).Error; err != nil {
		logx.WithContext(ctx).Errorf("query menus failed: %v", err)
		return nil, status.Error(codes.Internal, "query menus failed")
	}

	list := make([]*systemv1.MenuInfo, 0, len(menus))
	for i := range menus {
		list = append(list, toMenuInfo(&menus[i]))
	}

	return &systemv1.GetMenuTreeResponse{
		List: list,
	}, nil
}

// SaveMenu 新增或更新菜单/按钮。
func (s *SystemServer) SaveMenu(ctx context.Context, req *systemv1.SaveMenuRequest) (*systemv1.SaveMenuResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	category := int(req.Category)
	if category == 0 {
		category = 1
	}

	if req.Id == 0 {
		name := strings.TrimSpace(req.Name)
		if name == "" {
			return nil, status.Error(codes.InvalidArgument, "menu name cannot be empty")
		}
		menu := model.Menu{
			ID:        snowflake.ID(snowflake.GenID()),
			ParentID:  snowflake.ID(req.ParentId),
			Code:      strings.TrimSpace(req.Code),
			Name:      name,
			Path:      strings.TrimSpace(req.Path),
			Component: strings.TrimSpace(req.Component),
			Icon:      strings.TrimSpace(req.Icon),
			Sort:      int(req.Sort),
			Category:  category,
			Action:    strings.TrimSpace(req.Action),
		}
		if err := s.svcCtx.DB.WithContext(ctx).Create(&menu).Error; err != nil {
			logx.WithContext(ctx).Errorf("create menu failed: %v", err)
			return nil, status.Error(codes.Internal, "create menu failed")
		}
		s.publishRBACChanged(ctx, "save_menu", nil, nil)
		return &systemv1.SaveMenuResponse{Id: menu.ID.Int64()}, nil
	}

	var existing model.Menu
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.Id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "菜单不存在")
		}
		logx.WithContext(ctx).Errorf("query menu for update failed: %v", err)
		return nil, status.Error(codes.Internal, "query menu failed")
	}

	updates := map[string]any{
		"parent_id": snowflake.ID(req.ParentId),
		"code":      strings.TrimSpace(req.Code),
		"name":      strings.TrimSpace(req.Name),
		"path":      strings.TrimSpace(req.Path),
		"component": strings.TrimSpace(req.Component),
		"icon":      strings.TrimSpace(req.Icon),
		"sort":      int(req.Sort),
		"category":  category,
		"action":    strings.TrimSpace(req.Action),
	}
	if err := s.svcCtx.DB.WithContext(ctx).Model(&model.Menu{}).Where("id = ? AND is_deleted = 0", req.Id).Updates(updates).Error; err != nil {
		logx.WithContext(ctx).Errorf("update menu failed: %v", err)
		return nil, status.Error(codes.Internal, "update menu failed")
	}

	s.publishRBACChanged(ctx, "save_menu", nil, nil)
	return &systemv1.SaveMenuResponse{Id: req.Id}, nil
}

// DeleteMenus 批量逻辑删除菜单/按钮。
func (s *SystemServer) DeleteMenus(ctx context.Context, req *systemv1.DeleteMenusRequest) (*systemv1.DeleteMenusResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil || len(req.Ids) == 0 {
		return &systemv1.DeleteMenusResponse{Affected: 0}, nil
	}

	res := s.svcCtx.DB.WithContext(ctx).Model(&model.Menu{}).Where("id IN ? AND is_deleted = 0", req.Ids).Update("is_deleted", 1)
	if res.Error != nil {
		logx.WithContext(ctx).Errorf("delete menus failed: %v", res.Error)
		return nil, status.Error(codes.Internal, "delete menus failed")
	}

	if res.RowsAffected > 0 {
		s.publishRBACChanged(ctx, "delete_menus", nil, nil)
	}
	return &systemv1.DeleteMenusResponse{Affected: res.RowsAffected}, nil
}

// ListDicts 分页查询字典列表。
func (s *SystemServer) ListDicts(ctx context.Context, req *systemv1.ListDictsRequest) (*systemv1.ListDictsResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	current, size := normalizePage(req.GetCurrent(), req.GetSize())

	query := s.svcCtx.DB.WithContext(ctx).Model(&model.Dict{}).Where("is_deleted = 0")
	if code := strings.TrimSpace(req.GetCode()); code != "" {
		query = query.Where("code LIKE ?", "%"+code+"%")
	}
	if val := strings.TrimSpace(req.GetDictValue()); val != "" {
		query = query.Where("dict_value LIKE ?", "%"+val+"%")
	}
	if req.GetParentId() > 0 {
		query = query.Where("parent_id = ?", req.GetParentId())
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		logx.WithContext(ctx).Errorf("count dicts failed: %v", err)
		return nil, status.Error(codes.Internal, "count dicts failed")
	}

	var dicts []model.Dict
	offset := int((current - 1) * size)
	if err := query.Order("sort asc, id asc").Offset(offset).Limit(int(size)).Find(&dicts).Error; err != nil {
		logx.WithContext(ctx).Errorf("list dicts failed: %v", err)
		return nil, status.Error(codes.Internal, "list dicts failed")
	}

	records := make([]*systemv1.DictInfo, 0, len(dicts))
	for i := range dicts {
		d := dicts[i]
		records = append(records, &systemv1.DictInfo{
			Id:        d.ID.Int64(),
			ParentId:  d.ParentID.Int64(),
			Code:      d.Code,
			DictKey:   d.DictKey,
			DictValue: d.DictValue,
			Sort:      int32(d.Sort),
			Remark:    d.Remark,
		})
	}

	return &systemv1.ListDictsResponse{
		Records: records,
		Total:   total,
		Size:    size,
		Current: current,
	}, nil
}

// SaveDict 新增或更新字典项。
func (s *SystemServer) SaveDict(ctx context.Context, req *systemv1.SaveDictRequest) (*systemv1.SaveDictResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	if req.Id == 0 {
		code := strings.TrimSpace(req.Code)
		if code == "" {
			return nil, status.Error(codes.InvalidArgument, "dict code cannot be empty")
		}
		dict := model.Dict{
			ID:        snowflake.ID(snowflake.GenID()),
			ParentID:  snowflake.ID(req.ParentId),
			Code:      code,
			DictKey:   strings.TrimSpace(req.DictKey),
			DictValue: strings.TrimSpace(req.DictValue),
			Sort:      int(req.Sort),
			Remark:    strings.TrimSpace(req.Remark),
		}
		if err := s.svcCtx.DB.WithContext(ctx).Create(&dict).Error; err != nil {
			logx.WithContext(ctx).Errorf("create dict failed: %v", err)
			return nil, status.Error(codes.Internal, "create dict failed")
		}
		return &systemv1.SaveDictResponse{Id: dict.ID.Int64()}, nil
	}

	var existing model.Dict
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.Id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "字典不存在")
		}
		logx.WithContext(ctx).Errorf("query dict for update failed: %v", err)
		return nil, status.Error(codes.Internal, "query dict failed")
	}

	updates := map[string]any{
		"parent_id":  snowflake.ID(req.ParentId),
		"code":       strings.TrimSpace(req.Code),
		"dict_key":   strings.TrimSpace(req.DictKey),
		"dict_value": strings.TrimSpace(req.DictValue),
		"sort":       int(req.Sort),
		"remark":     strings.TrimSpace(req.Remark),
	}
	if err := s.svcCtx.DB.WithContext(ctx).Model(&model.Dict{}).Where("id = ? AND is_deleted = 0", req.Id).Updates(updates).Error; err != nil {
		logx.WithContext(ctx).Errorf("update dict failed: %v", err)
		return nil, status.Error(codes.Internal, "update dict failed")
	}

	return &systemv1.SaveDictResponse{Id: req.Id}, nil
}

// DeleteDicts 批量逻辑删除字典项。
func (s *SystemServer) DeleteDicts(ctx context.Context, req *systemv1.DeleteDictsRequest) (*systemv1.DeleteDictsResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil || len(req.Ids) == 0 {
		return &systemv1.DeleteDictsResponse{Affected: 0}, nil
	}

	res := s.svcCtx.DB.WithContext(ctx).Model(&model.Dict{}).Where("id IN ? AND is_deleted = 0", req.Ids).Update("is_deleted", 1)
	if res.Error != nil {
		logx.WithContext(ctx).Errorf("delete dicts failed: %v", res.Error)
		return nil, status.Error(codes.Internal, "delete dicts failed")
	}

	return &systemv1.DeleteDictsResponse{Affected: res.RowsAffected}, nil
}

// ListAuditLogs 分页查询 API 操作审计日志列表（自动遵循多租户隔离）。
func (s *SystemServer) ListAuditLogs(ctx context.Context, req *systemv1.ListAuditLogsRequest) (*systemv1.ListAuditLogsResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	current, size := normalizePage(req.GetCurrent(), req.GetSize())

	query := s.svcCtx.DB.WithContext(ctx).Model(&model.AuditLog{})
	if acc := strings.TrimSpace(req.GetAccount()); acc != "" {
		query = query.Where("account LIKE ?", "%"+acc+"%")
	}
	if m := strings.TrimSpace(req.GetMethod()); m != "" {
		query = query.Where("method = ?", strings.ToUpper(m))
	}
	if p := strings.TrimSpace(req.GetPath()); p != "" {
		query = query.Where("path LIKE ?", "%"+p+"%")
	}
	if req.GetStatusCode() > 0 {
		query = query.Where("status_code = ?", req.GetStatusCode())
	}
	if tid := strings.TrimSpace(req.GetTraceId()); tid != "" {
		query = query.Where("trace_id = ?", tid)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		logx.WithContext(ctx).Errorf("count audit logs failed: %v", err)
		return nil, status.Error(codes.Internal, "count audit logs failed")
	}

	var logs []model.AuditLog
	offset := int((current - 1) * size)
	if err := query.Order("id desc").Offset(offset).Limit(int(size)).Find(&logs).Error; err != nil {
		logx.WithContext(ctx).Errorf("list audit logs failed: %v", err)
		return nil, status.Error(codes.Internal, "list audit logs failed")
	}

	records := make([]*systemv1.AuditLogInfo, 0, len(logs))
	for _, item := range logs {
		createdAtStr := ""
		if !item.CreatedAt.IsZero() {
			createdAtStr = item.CreatedAt.Format("2006-01-02 15:04:05")
		}
		records = append(records, &systemv1.AuditLogInfo{
			Id:          item.ID.Int64(),
			TenantId:    item.TenantID,
			ServiceName: item.ServiceName,
			UserId:      item.UserID.Int64(),
			Account:     item.Account,
			Method:      item.Method,
			Path:        item.Path,
			QueryParams: item.QueryParams,
			RequestBody: item.RequestBody,
			StatusCode:  int32(item.StatusCode),
			DurationMs:  item.DurationMs,
			ClientIp:    item.ClientIP,
			TraceId:     item.TraceID,
			CreatedAt:   createdAtStr,
		})
	}

	return &systemv1.ListAuditLogsResponse{
		Records: records,
		Total:   total,
		Size:    size,
		Current: current,
	}, nil
}

// ListParams 分页查询系统动态参数列表。
func (s *SystemServer) ListParams(ctx context.Context, req *systemv1.ListParamsRequest) (*systemv1.ListParamsResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	current, size := normalizePage(req.GetCurrent(), req.GetSize())

	query := s.svcCtx.DB.WithContext(ctx).Model(&model.Param{}).Where("is_deleted = 0")
	if name := strings.TrimSpace(req.GetParamName()); name != "" {
		query = query.Where("param_name LIKE ?", "%"+name+"%")
	}
	if key := strings.TrimSpace(req.GetParamKey()); key != "" {
		query = query.Where("param_key LIKE ?", "%"+key+"%")
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		logx.WithContext(ctx).Errorf("count params failed: %v", err)
		return nil, status.Error(codes.Internal, "count params failed")
	}

	var params []model.Param
	offset := int((current - 1) * size)
	if err := query.Order("id desc").Offset(offset).Limit(int(size)).Find(&params).Error; err != nil {
		logx.WithContext(ctx).Errorf("list params failed: %v", err)
		return nil, status.Error(codes.Internal, "list params failed")
	}

	records := make([]*systemv1.ParamInfo, 0, len(params))
	for _, p := range params {
		records = append(records, &systemv1.ParamInfo{
			Id:         p.ID.Int64(),
			TenantId:   p.TenantID,
			ParamName:  p.ParamName,
			ParamKey:   p.ParamKey,
			ParamValue: p.ParamValue,
			Remark:     p.Remark,
		})
	}

	return &systemv1.ListParamsResponse{
		Records: records,
		Total:   total,
		Size:    size,
		Current: current,
	}, nil
}

// SaveParam 新增或修改系统动态参数并同步刷新缓存。
func (s *SystemServer) SaveParam(ctx context.Context, req *systemv1.SaveParamRequest) (*systemv1.SaveParamResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}

	paramName := strings.TrimSpace(req.ParamName)
	paramKey := strings.TrimSpace(req.ParamKey)
	paramValue := strings.TrimSpace(req.ParamValue)
	remark := strings.TrimSpace(req.Remark)

	if req.Id == 0 {
		if paramName == "" || paramKey == "" {
			return nil, status.Error(codes.InvalidArgument, "param_name and param_key cannot be empty")
		}
		param := model.Param{
			ID:         snowflake.ID(snowflake.GenID()),
			ParamName:  paramName,
			ParamKey:   paramKey,
			ParamValue: paramValue,
			Remark:     remark,
			Status:     1,
		}
		if err := s.svcCtx.DB.WithContext(ctx).Create(&param).Error; err != nil {
			logx.WithContext(ctx).Errorf("create param failed: %v", err)
			return nil, status.Error(codes.Internal, "create param failed")
		}
		if s.svcCtx.ParamCache != nil {
			_ = s.svcCtx.ParamCache.Set(ctx, tenant.FromTenant(ctx)+":"+paramKey, paramValue, 0)
		}
		return &systemv1.SaveParamResponse{Id: param.ID.Int64()}, nil
	}

	var existing model.Param
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.Id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "系统参数不存在")
		}
		return nil, status.Error(codes.Internal, "query param failed")
	}

	updates := map[string]any{
		"param_name":  paramName,
		"param_key":   paramKey,
		"param_value": paramValue,
		"remark":      remark,
	}
	if err := s.svcCtx.DB.WithContext(ctx).Model(&model.Param{}).Where("id = ? AND is_deleted = 0", req.Id).Updates(updates).Error; err != nil {
		logx.WithContext(ctx).Errorf("update param failed: %v", err)
		return nil, status.Error(codes.Internal, "update param failed")
	}

	if s.svcCtx.ParamCache != nil {
		tID := tenant.FromTenant(ctx)
		_ = s.svcCtx.ParamCache.Delete(ctx, tID+":"+existing.ParamKey)
		if paramKey != "" {
			_ = s.svcCtx.ParamCache.Set(ctx, tID+":"+paramKey, paramValue, 0)
		}
	}
	return &systemv1.SaveParamResponse{Id: req.Id}, nil
}

// DeleteParams 批量逻辑删除系统动态参数并清理缓存。
func (s *SystemServer) DeleteParams(ctx context.Context, req *systemv1.DeleteParamsRequest) (*systemv1.DeleteParamsResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "system database not initialized")
	}
	if req == nil || len(req.Ids) == 0 {
		return &systemv1.DeleteParamsResponse{Affected: 0}, nil
	}

	res := s.svcCtx.DB.WithContext(ctx).Model(&model.Param{}).Where("id IN ? AND is_deleted = 0", req.Ids).Update("is_deleted", 1)
	if res.Error != nil {
		logx.WithContext(ctx).Errorf("delete params failed: %v", res.Error)
		return nil, status.Error(codes.Internal, "delete params failed")
	}

	if s.svcCtx.ParamCache != nil {
		_ = s.svcCtx.ParamCache.DeleteByPrefix(ctx, tenant.FromTenant(ctx)+":")
	}
	return &systemv1.DeleteParamsResponse{Affected: res.RowsAffected}, nil
}

// publishRBACChanged 向消息队列发布 RBAC 权限变更广播事件，通知所有网关实例实时失效权限缓存。
func (s *SystemServer) publishRBACChanged(ctx context.Context, action string, roleIDs []int64, userIDs []int64) {
	if s == nil || s.svcCtx == nil {
		return
	}
	broker := s.svcCtx.MQBroker
	if broker == nil {
		broker = mq.DefaultBroker()
	}
	tenantID := tenant.FromTenant(ctx)
	ev := event.RBACChangedEvent{
		TenantID: tenantID,
		Action:   action,
		RoleIDs:  roleIDs,
		UserIDs:  userIDs,
	}
	if err := broker.Publish(ctx, event.TopicRBACChanged, ev, mq.WithKey(tenantID)); err != nil {
		logx.WithContext(ctx).Errorf("[system-rbac] publish rbac changed event failed: %v", err)
	}
}

func normalizePage(current, size int64) (int64, int64) {
	if current <= 0 {
		current = 1
	}
	if size <= 0 {
		size = 10
	}
	if size > 500 {
		size = 500
	}
	return current, size
}

func toUserInfo(u *model.User) *systemv1.UserInfo {
	return &systemv1.UserInfo{
		Id:       u.ID.Int64(),
		TenantId: u.TenantID,
		Account:  u.Account,
		Name:     u.Name,
		RealName: u.RealName,
		Email:    u.Email,
		Phone:    u.Phone,
		DeptId:   u.DeptID.Int64(),
		RoleId:   u.RoleID,
		Status:   int32(u.Status),
	}
}

func toMenuInfo(m *model.Menu) *systemv1.MenuInfo {
	return &systemv1.MenuInfo{
		Id:        m.ID.Int64(),
		ParentId:  m.ParentID.Int64(),
		Code:      m.Code,
		Name:      m.Name,
		Path:      m.Path,
		Component: m.Component,
		Icon:      m.Icon,
		Sort:      int32(m.Sort),
		Category:  int32(m.Category),
		Action:    m.Action,
	}
}

func buildDeptTree(depts []model.Dept) []*systemv1.DeptInfo {
	if len(depts) == 0 {
		return nil
	}

	nodes := make([]*systemv1.DeptInfo, len(depts))
	nodeMap := make(map[int64]*systemv1.DeptInfo, len(depts))
	for i, d := range depts {
		node := &systemv1.DeptInfo{
			Id:       d.ID.Int64(),
			TenantId: d.TenantID,
			ParentId: d.ParentID.Int64(),
			DeptName: d.DeptName,
			FullName: d.FullName,
			Sort:     int32(d.Sort),
			Remark:   d.Remark,
		}
		nodes[i] = node
		nodeMap[node.Id] = node
	}

	var roots []*systemv1.DeptInfo
	for _, node := range nodes {
		if node.ParentId != 0 {
			if parent, ok := nodeMap[node.ParentId]; ok {
				parent.Children = append(parent.Children, node)
				continue
			}
		}
		roots = append(roots, node)
	}
	return roots
}

func parseRoleIDList(raw string) []int64 {
	parts := strings.Split(raw, ",")
	ids := make([]int64, 0, len(parts))
	seen := make(map[int64]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if id, err := strconv.ParseInt(p, 10, 64); err == nil && id > 0 {
			if _, exists := seen[id]; !exists {
				seen[id] = struct{}{}
				ids = append(ids, id)
			}
		}
	}
	return ids
}

func extractMenuIDs(roleMenus []model.RoleMenu) []int64 {
	ids := make([]int64, 0, len(roleMenus))
	seen := make(map[int64]struct{}, len(roleMenus))
	for _, rm := range roleMenus {
		id := rm.MenuID.Int64()
		if id > 0 {
			if _, exists := seen[id]; !exists {
				seen[id] = struct{}{}
				ids = append(ids, id)
			}
		}
	}
	return ids
}

// isSuperAdmin 判断逗号分隔的角色字符串中是否包含超级管理员角色（administrator 或 admin）。
func isSuperAdmin(roleNames string) bool {
	for _, r := range strings.Split(roleNames, ",") {
		lower := strings.ToLower(strings.TrimSpace(r))
		if lower == "administrator" || lower == "admin" {
			return true
		}
	}
	return false
}

// resolveUserDataScope 根据当前调用者 Claims 与角色配置解析最高级别的行级数据权限并注入 Context。
func (s *SystemServer) resolveUserDataScope(ctx context.Context) context.Context {
	if _, exists := database.FromDataScope(ctx); exists {
		return ctx
	}
	claims := jwt.FromClaims(ctx)
	if claims == nil || isSuperAdmin(claims.RoleName) {
		return ctx
	}
	roleIDs := parseRoleIDList(claims.RoleID)
	if len(roleIDs) == 0 || s.svcCtx == nil || s.svcCtx.DB == nil {
		return ctx
	}

	var roles []model.Role
	if err := s.svcCtx.DB.WithContext(ctx).Where("id IN ? AND is_deleted = 0", roleIDs).Find(&roles).Error; err != nil || len(roles) == 0 {
		return ctx
	}

	// 计算最高数据权限级别（数字越小权限范围越大；DataScopeType <= 0 视为 1:全部数据）
	bestScope := database.DataScopeType(0)
	var customDeptIDs []int64
	customSeen := make(map[int64]struct{})
	for _, r := range roles {
		if isSuperAdmin(r.RoleAlias) {
			return ctx
		}
		st := database.DataScopeType(r.DataScopeType)
		if st <= 0 {
			st = database.DataScopeAll
		}
		if st == database.DataScopeAll {
			return ctx
		}
		if st == database.DataScopeCustom {
			for _, did := range parseRoleIDList(r.DataScopeDeptIDs) {
				if _, ok := customSeen[did]; !ok {
					customSeen[did] = struct{}{}
					customDeptIDs = append(customDeptIDs, did)
				}
			}
		}
		if bestScope == 0 || st < bestScope {
			bestScope = st
		}
	}

	switch bestScope {
	case database.DataScopeCustom:
		return database.WithDataScope(ctx, database.DataScope{
			Type:    database.DataScopeCustom,
			DeptID:  claims.DeptID,
			DeptIDs: customDeptIDs,
		})
	case database.DataScopeDept:
		return database.WithDataScope(ctx, database.DataScope{
			Type:   database.DataScopeDept,
			DeptID: claims.DeptID,
		})
	case database.DataScopeDeptAndChild:
		deptIDs := s.collectDeptAndChildIDs(ctx, claims.DeptID)
		return database.WithDataScope(ctx, database.DataScope{
			Type:    database.DataScopeDeptAndChild,
			DeptID:  claims.DeptID,
			DeptIDs: deptIDs,
		})
	case database.DataScopeSelf:
		return database.WithDataScope(ctx, database.DataScope{
			Type:   database.DataScopeSelf,
			UserID: claims.UserID,
		})
	default:
		return ctx
	}
}

// collectDeptAndChildIDs 递归收集当前租户下指定部门及其所有子孙部门的主键 ID 列表。
func (s *SystemServer) collectDeptAndChildIDs(ctx context.Context, rootDeptID int64) []int64 {
	if rootDeptID <= 0 {
		return nil
	}
	var depts []model.Dept
	if err := s.svcCtx.DB.WithContext(ctx).Where("is_deleted = 0").Find(&depts).Error; err != nil {
		return []int64{rootDeptID}
	}
	childrenMap := make(map[int64][]int64, len(depts))
	for _, d := range depts {
		pid := d.ParentID.Int64()
		childrenMap[pid] = append(childrenMap[pid], d.ID.Int64())
	}

	result := make([]int64, 0, 8)
	visited := make(map[int64]struct{})
	queue := []int64{rootDeptID}
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		if _, ok := visited[curr]; ok {
			continue
		}
		visited[curr] = struct{}{}
		result = append(result, curr)
		for _, childID := range childrenMap[curr] {
			if _, ok := visited[childID]; !ok {
				queue = append(queue, childID)
			}
		}
	}
	return result
}
