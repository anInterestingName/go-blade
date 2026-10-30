// Package server 实现 DeskService gRPC 服务契约。
package server

import (
	"context"
	"errors"
	"strings"
	"time"

	deskv1 "github.com/go-blade/api/proto/desk/v1"
	"github.com/go-blade/pkg/event"
	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/services/desk/internal/model"
	"github.com/go-blade/services/desk/internal/svc"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

// DeskServer 实现 deskv1.DeskServiceServer 接口契约。
type DeskServer struct {
	deskv1.UnimplementedDeskServiceServer
	svcCtx *svc.ServiceContext
}

// NewDeskServer 构造 DeskServer 实例。
func NewDeskServer(svcCtx *svc.ServiceContext) *DeskServer {
	return &DeskServer{
		svcCtx: svcCtx,
	}
}

// ListNotices 分页查询通知公告列表，支持按标题模糊搜索与按分类精确过滤。
func (s *DeskServer) ListNotices(ctx context.Context, req *deskv1.ListNoticesRequest) (*deskv1.ListNoticesResponse, error) {
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "desk database not initialized")
	}

	current := int64(1)
	size := int64(10)
	if req != nil {
		if req.Current > 0 {
			current = req.Current
		}
		if req.Size > 0 {
			size = req.Size
		}
		if size > 100 {
			size = 100
		}
	}

	db := s.svcCtx.DB.WithContext(ctx).Model(&model.Notice{}).Where("is_deleted = 0")
	if req != nil {
		if title := strings.TrimSpace(req.Title); title != "" {
			db = db.Where("title LIKE ?", "%"+title+"%")
		}
		if req.Category > 0 {
			db = db.Where("category = ?", req.Category)
		}
	}

	var total int64
	if err := db.Count(&total).Error; err != nil {
		logx.WithContext(ctx).Errorf("count notices failed: %v", err)
		return nil, status.Error(codes.Internal, "count notices failed")
	}

	var notices []model.Notice
	offset := int((current - 1) * size)
	if total > 0 {
		if err := db.Order("id desc").Offset(offset).Limit(int(size)).Find(&notices).Error; err != nil {
			logx.WithContext(ctx).Errorf("query notices failed: %v", err)
			return nil, status.Error(codes.Internal, "query notices failed")
		}
	}

	records := make([]*deskv1.NoticeInfo, 0, len(notices))
	for _, n := range notices {
		records = append(records, toProtoNotice(n))
	}

	return &deskv1.ListNoticesResponse{
		Records: records,
		Total:   total,
		Size:    size,
		Current: current,
	}, nil
}

// GetNotice 查询单条通知公告详情。
func (s *DeskServer) GetNotice(ctx context.Context, req *deskv1.GetNoticeRequest) (*deskv1.GetNoticeResponse, error) {
	if req == nil || req.Id <= 0 {
		return nil, status.Error(codes.InvalidArgument, "id must be greater than 0")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "desk database not initialized")
	}

	var n model.Notice
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.Id).First(&n).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "通知公告不存在")
		}
		logx.WithContext(ctx).Errorf("get notice failed: %v", err)
		return nil, status.Error(codes.Internal, "get notice failed")
	}

	return &deskv1.GetNoticeResponse{
		Notice: toProtoNotice(n),
	}, nil
}

// SaveNotice 新增或编辑通知公告。
func (s *DeskServer) SaveNotice(ctx context.Context, req *deskv1.SaveNoticeRequest) (*deskv1.SaveNoticeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be empty")
	}
	title := strings.TrimSpace(req.Title)
	if title == "" {
		return nil, status.Error(codes.InvalidArgument, "title cannot be empty")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "desk database not initialized")
	}

	category := int(req.Category)
	if category <= 0 {
		category = 1
	}
	noticeStatus := int(req.Status)
	if noticeStatus <= 0 {
		noticeStatus = 1
	}
	releaseTime := strings.TrimSpace(req.ReleaseTime)
	if releaseTime == "" {
		releaseTime = time.Now().Format("2006-01-02 15:04:05")
	}

	if req.Id <= 0 {
		// 新增通知公告
		notice := model.Notice{
			ID:          snowflake.NextID(),
			Title:       title,
			Category:    category,
			ReleaseTime: releaseTime,
			Content:     req.Content,
			Status:      noticeStatus,
		}
		if err := s.svcCtx.DB.WithContext(ctx).Create(&notice).Error; err != nil {
			logx.WithContext(ctx).Errorf("create notice failed: %v", err)
			return nil, status.Error(codes.Internal, "create notice failed")
		}
		s.publishNoticeEvent(ctx, notice.ID, title, int32(category), releaseTime)
		return &deskv1.SaveNoticeResponse{
			Id: notice.ID.Int64(),
		}, nil
	}

	// 修改已有通知公告
	var existing model.Notice
	if err := s.svcCtx.DB.WithContext(ctx).Where("id = ? AND is_deleted = 0", req.Id).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "通知公告不存在")
		}
		logx.WithContext(ctx).Errorf("query notice for update failed: %v", err)
		return nil, status.Error(codes.Internal, "query notice failed")
	}

	updates := map[string]any{
		"title":        title,
		"category":     category,
		"release_time": releaseTime,
		"content":      req.Content,
		"status":       noticeStatus,
	}
	if err := s.svcCtx.DB.WithContext(ctx).Model(&existing).Updates(updates).Error; err != nil {
		logx.WithContext(ctx).Errorf("update notice failed: %v", err)
		return nil, status.Error(codes.Internal, "update notice failed")
	}

	s.publishNoticeEvent(ctx, existing.ID, title, int32(category), releaseTime)
	return &deskv1.SaveNoticeResponse{
		Id: existing.ID.Int64(),
	}, nil
}

// publishNoticeEvent 在通知公告新增或修改成功后向消息队列发布实时广播事件。
func (s *DeskServer) publishNoticeEvent(ctx context.Context, id snowflake.ID, title string, category int32, releaseTime string) {
	if s == nil || s.svcCtx == nil || s.svcCtx.MQBroker == nil {
		return
	}
	tenantID := tenant.FromTenant(ctx)
	ev := event.NoticePublishedEvent{
		ID:          id,
		TenantID:    tenantID,
		Title:       title,
		Category:    category,
		ReleaseTime: releaseTime,
	}
	_ = s.svcCtx.MQBroker.Publish(ctx, event.TopicNoticePublished, ev, mq.WithKey(tenantID))
}

// DeleteNotices 批量逻辑软删除通知公告（将 is_deleted 置为 1）。
func (s *DeskServer) DeleteNotices(ctx context.Context, req *deskv1.DeleteNoticesRequest) (*deskv1.DeleteNoticesResponse, error) {
	if req == nil || len(req.Ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ids cannot be empty")
	}
	if s.svcCtx.DB == nil {
		return nil, status.Error(codes.Internal, "desk database not initialized")
	}

	validIDs := make([]int64, 0, len(req.Ids))
	for _, id := range req.Ids {
		if id > 0 {
			validIDs = append(validIDs, id)
		}
	}
	if len(validIDs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no valid notice ids provided")
	}

	tx := s.svcCtx.DB.WithContext(ctx).
		Model(&model.Notice{}).
		Where("id IN ? AND is_deleted = 0", validIDs).
		Update("is_deleted", 1)
	if tx.Error != nil {
		logx.WithContext(ctx).Errorf("delete notices failed: %v", tx.Error)
		return nil, status.Error(codes.Internal, "delete notices failed")
	}

	return &deskv1.DeleteNoticesResponse{
		Affected: tx.RowsAffected,
	}, nil
}

func toProtoNotice(n model.Notice) *deskv1.NoticeInfo {
	createTime := ""
	if !n.CreateTime.IsZero() {
		createTime = n.CreateTime.Format("2006-01-02 15:04:05")
	}
	return &deskv1.NoticeInfo{
		Id:          n.ID.Int64(),
		TenantId:    n.TenantID,
		Title:       n.Title,
		Category:    int32(n.Category),
		ReleaseTime: n.ReleaseTime,
		Content:     n.Content,
		Status:      int32(n.Status),
		CreateUser:  n.CreateUser.Int64(),
		CreateDept:  n.CreateDept.Int64(),
		CreateTime:  createTime,
	}
}
