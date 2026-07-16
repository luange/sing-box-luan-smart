package adapter

type GenerationLease interface {
	Release()
}

type GenerationLeaser interface {
	AcquireGeneration() (GenerationLease, error)
}

type GenerationLifecycle interface {
	OnGenerationPublish()
	OnGenerationRetire()
}

func AcquireGeneration(service any) (GenerationLease, error) {
	if leaser, loaded := service.(GenerationLeaser); loaded {
		return leaser.AcquireGeneration()
	}
	return nopGenerationLease{}, nil
}

type nopGenerationLease struct{}

func (nopGenerationLease) Release() {}
