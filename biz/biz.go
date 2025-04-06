package biz

type BizService struct {
}

func NewBizService() *BizService {
	return &BizService{}
}

func (s *BizService) Add() error {
	return nil
}
func (s *BizService) Check() error {
	return nil
}
func (s *BizService) Test() error {
	return nil
}
