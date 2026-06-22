package orchestration

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-chorus-x/orchestration/databricksactivity"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-chorus/orchestration/executable/factory"
	"github.com/rs/zerolog/log"
)

func init() {
	const semLogContext = "tpm-chorus-x::init"
	log.Info().Msg(semLogContext)
	var err error
	err = factory.RegisterActivityFactory(databricksactivity.ActivityType, databricksactivity.NewDatabricksActivity)
	if nil != err {
		log.Error().Err(err).Msg(semLogContext + " activity registry initialization error")
	}
}
