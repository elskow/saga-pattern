package orders

import "context"

func (s *Service) PublishPending(ctx context.Context) error {
	return s.runtime.PublishPending(ctx)
}
