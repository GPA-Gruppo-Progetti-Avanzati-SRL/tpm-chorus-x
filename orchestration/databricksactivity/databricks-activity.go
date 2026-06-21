package databricksactivity

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-cache-common/cachelks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-cache-common/cacheoperation"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-chorus/orchestration/config"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-chorus/orchestration/executable"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-chorus/orchestration/wfcase"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-chorus/orchestration/wfcase/wfexpressions"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-chorus/smperror"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	varResolver "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/vars"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-databricks-common/dbricksLks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-databricks-common/sql"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/har"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const (
	ActivityType         = "databricks-activity"
	SemLogPackageContext = "databricks-activity"
	SemLogActivity       = "activity"

	OperationResultMatchedCountPropertyVarName = "matched-count"
)

type DatabricksActivity struct {
	executable.Activity
	definition Definition
}

func NewDatabricksActivity(item config.Configurable, refs config.DataReferences) (executable.Executable, error) {
	const semLogContext = SemLogPackageContext + "::new"
	var err error

	ma := &DatabricksActivity{}
	ma.Cfg = item
	ma.Refs = refs

	maCfg := item.(*config.GenericActivity)
	ma.definition, err = UnmarshalDefinition(maCfg.Definition, refs)
	if err != nil {
		return nil, err
	}

	return ma, nil
}

func (a *DatabricksActivity) Execute(wfc *wfcase.WfCase) error {

	const semLogContext = SemLogPackageContext + "::execute"
	var err error

	if !a.IsEnabled(wfc) {
		log.Trace().Str("activity", a.Name()).Str("type", string(ActivityType)).Msg(semLogContext + " activity not enabled")
		return nil
	}

	log.Info().Str(SemLogActivity, a.Name()).Msg(semLogContext + " start")
	defer log.Info().Str(SemLogActivity, a.Name()).Msg(semLogContext + " end")

	tcfg, ok := a.Cfg.(*config.GenericActivity)
	if !ok {
		err = fmt.Errorf("this is weird %T is not %s config type", a.Cfg, config.GenericActivityType)
		wfc.AddBreadcrumb(a.Name(), a.Cfg.Description(), err)
		log.Error().Err(err).Msg(semLogContext)
		return smperror.NewExecutableServerError(smperror.WithErrorAmbit(a.Name()), smperror.WithErrorMessage(err.Error()))
	}

	err = tcfg.WfCaseDeadlineExceeded(wfc.RequestTiming, wfc.RequestDeadline)
	if err != nil {
		return smperror.NewExecutableServerError(smperror.WithErrorAmbit(a.Name()), smperror.WithErrorMessage(err.Error()))
	}

	activityBegin := time.Now()
	defer func(begin time.Time) {
		wfc.RequestTiming += time.Since(begin)
		log.Info().Str(SemLogActivity, a.Name()).Float64("wfc-timing.s", wfc.RequestTiming.Seconds()).Float64("deadline.s", wfc.RequestDeadline.Seconds()).Msg(semLogContext + " - wfc timing")
	}(activityBegin)

	_, _, err = a.MetricsGroup()
	if err != nil {
		log.Error().Err(err).Interface("metrics-config", a.Cfg.MetricsConfig()).Msg(semLogContext + " cannot found metrics group")
		return smperror.NewExecutableServerError(smperror.WithErrorAmbit(a.Name()), smperror.WithErrorMessage(err.Error()))
	}

	if len(tcfg.ProcessVars) > 0 {
		expressionCtx, err := wfc.ResolveHarEntryReferenceByName(a.Cfg.ExpressionContextNameStringReference())
		if err != nil {
			log.Error().Err(err).Str(SemLogActivity, a.Name()).Msg(semLogContext)
			return err
		}
		log.Trace().Str(SemLogActivity, a.Name()).Msg(semLogContext + " start")

		err = wfc.SetVars(expressionCtx, tcfg.ProcessVars, "", false)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			wfc.AddBreadcrumb(a.Name(), a.Cfg.Description(), err)
			return smperror.NewExecutableServerError(smperror.WithErrorAmbit(a.Name()), smperror.WithErrorMessage(err.Error()))
		}
	}

	beginOf := time.Now()
	metricsLabels := a.MetricsLabels()
	defer func() { a.SetMetrics(beginOf, metricsLabels) }()

	resolver, err := a.GetEvaluator(wfc)
	if err != nil {
		return smperror.NewExecutableServerError(smperror.WithErrorAmbit(a.Name()), smperror.WithErrorMessage(err.Error()))
	}

	var harResponse *har.Response
	var cacheCfg config.CacheConfig
	var cacheEnabled bool
	cacheEnabled, err = a.definition.CacheConfig.Enabled()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}
	if cacheEnabled {
		cacheCfg, err = a.resolveCacheConfig(wfc, resolver, a.definition.CacheConfig, a.Refs)
		if err != nil {
			// The get of the cache triggers an error only.
			log.Error().Err(err).Msg(semLogContext)
		} else {
			harResponse, err = a.resolveResponseFromCache(wfc, cacheCfg)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				if harResponse == nil {
					return smperror.NewExecutableServerError(smperror.WithErrorAmbit(a.Name()), smperror.WithErrorMessage(err.Error()))
				}
			}
		}
	}

	var mongoError int
	if harResponse == nil || harResponse.Status != http.StatusOK {
		op, err := a.resolveOperation(wfc, a.definition.Statement)
		if err != nil {
			wfc.AddBreadcrumb(a.Name(), a.Cfg.Description(), err)
			return smperror.NewExecutableServerError(smperror.WithErrorAmbit(a.Name()), smperror.WithErrorMessage(err.Error()))
		}

		req, err := a.newRequestDefinition(wfc, op)
		if err != nil {
			wfc.AddBreadcrumb(a.Name(), a.Cfg.Description(), err)
			metricsLabels[MetricIdStatusCode] = "500"
			// See defer a.SetMetrics(beginOf, metricsLabels)
			return smperror.NewExecutableServerError(smperror.WithErrorAmbit(a.Name()), smperror.WithStep(a.Name()), smperror.WithErrorMessage(err.Error()))
		}

		_ = wfc.SetHarEntryRequest(a.Name(), req, a.definition.PII)

		harResponse, mongoError, err = a.Invoke(wfc, op)
		if err != nil {
			log.Error().Err(err).Str(SemLogActivity, a.Name()).Msg(semLogContext)
		}

		if harResponse != nil {
			_ = wfc.SetHarEntryResponse(a.Name(), harResponse, a.definition.PII)
			metricsLabels[MetricIdStatusCode] = fmt.Sprint(mongoError)

			if cacheEnabled && harResponse.Status == http.StatusOK {
				err = a.saveResponseToCache(cacheCfg, harResponse.Content.Data)
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
				}
			}
		}
	}

	statusToRemap := harResponse.Status
	if harResponse.Status != http.StatusOK && mongoError != 0 {
		statusToRemap = mongoError
	}
	remappedStatusCode, err := a.ProcessResponseActionByStatusCode(
		statusToRemap, a.Name(), a.Name(), wfc, nil, wfcase.HarEntryReference{Name: a.Name(), UseResponse: true}, a.definition.OnResponseActions, false)
	if remappedStatusCode > 0 {
		metricsLabels[MetricIdStatusCode] = fmt.Sprint(remappedStatusCode)
	}
	if err != nil {
		wfc.AddBreadcrumb(a.Name(), a.Cfg.Description(), err)
		return err
	}

	// See defer _ = a.SetMetrics(beginOf, metricsLabels)
	wfc.AddBreadcrumb(a.Name(), a.Cfg.Description(), nil)

	return err
}

func (a *DatabricksActivity) resolveOperation(wfc *wfcase.WfCase, statement Statement) (*sql.Operation, error) {

	resolver, err := a.GetEvaluator(wfc)
	if err != nil {
		return nil, err
	}

	s, _, err := varResolver.ResolveVariables(statement.Text, varResolver.SimpleVariableReference, resolver.VarResolverFunc, true)
	if err != nil {
		return nil, err
	}

	b1, err := wfc.ProcessTemplate(s)
	if err != nil {
		return nil, err
	}

	return sql.NewOperation(statement.OpType, string(b1), statement.MustFind)
}

func (a *DatabricksActivity) Invoke(wfc *wfcase.WfCase, op *sql.Operation) (*har.Response, int, error) {

	const semLogContext = SemLogPackageContext + "::invoke"
	lks, err := dbricksLks.GetLinkedService(a.definition.LksName)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		r := har.NewResponse(http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError), "text/plain", []byte(err.Error()), nil)
		return r, http.StatusInternalServerError, err
	}

	opResult, resp, err := op.Execute(context.Background(), lks)
	var r *har.Response
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		err = util.NewError(strconv.Itoa(http.StatusInternalServerError), err)
		r = har.NewResponse(http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError), "text/plain", []byte(err.Error()), nil)
		return r, http.StatusInternalServerError, err
	}

	on200ActionNdx := a.definition.OnResponseActions.FindByStatusCode(http.StatusOK)
	if on200ActionNdx >= 0 && len(a.definition.OnResponseActions[on200ActionNdx].Properties) > 0 {
		onResponseProperties := a.definition.OnResponseActions[on200ActionNdx].Properties
		if varName, ok := onResponseProperties[OperationResultMatchedCountPropertyVarName]; ok {
			wfc.Vars.V[varName] = opResult.MatchedCOunt
		}
	}

	r = &har.Response{
		Status:      http.StatusOK,
		HTTPVersion: "1.1",
		StatusText:  http.StatusText(http.StatusOK),
		HeadersSize: -1,
		BodySize:    int64(len(resp)),
		Cookies:     []har.Cookie{},
		Headers:     []har.NameValuePair{},
		Content: &har.Content{
			MimeType: "application/json",
			Size:     int64(len(resp)),
			Data:     resp,
		},
	}

	return r, 0, nil
}

func (a *DatabricksActivity) newRequestDefinition(wfc *wfcase.WfCase, op *sql.Operation) (*har.Request, error) {

	var opts []har.RequestOption

	ub := har.UrlBuilder{}
	ub.WithPort(443)
	ub.WithScheme("https")
	ub.WithHostname("xyz.azuredatabricks.net")
	ub.WithPath(fmt.Sprintf("/%s/%s/%s", string(ActivityType), string(op.Type), a.Name()))
	opts = append(opts, har.WithMethod("POST"))
	opts = append(opts, har.WithUrl(ub.Url()))
	opts = append(opts, har.WithBody([]byte(op.ToJsonString())))

	req := har.Request{
		HTTPVersion: "1.1",
		Cookies:     []har.Cookie{},
		QueryString: []har.NameValuePair{},
		HeadersSize: -1,
		Headers:     []har.NameValuePair{},
		BodySize:    -1,
	}
	for _, o := range opts {
		o(&req)
	}

	return &req, nil
}

const (
	MetricIdActivityType = "type"
	MetricIdActivityName = "name"
	MetricIdOpType       = "op-type"
	MetricIdStatusCode   = "status-code"
)

func (a *DatabricksActivity) MetricsLabels() prometheus.Labels {

	cfg := a.Cfg.(*config.GenericActivity)
	metricsLabels := prometheus.Labels{
		MetricIdActivityType: string(cfg.Type()),
		MetricIdActivityName: a.Name(),
		MetricIdOpType:       string(a.definition.Statement.OpType),
		MetricIdStatusCode:   "-1",
	}

	return metricsLabels
}

func (a *DatabricksActivity) resolveCacheConfig(wfc *wfcase.WfCase, resolver *wfexpressions.Evaluator, cacheConfig config.CacheConfig, refs config.DataReferences) (config.CacheConfig, error) {
	cfg := cacheConfig
	if refs.IsPresent(cacheConfig.Key) {
		if key, ok := refs.Find(cacheConfig.Key); ok {
			cfg.Key = string(key)
		}
	}

	s, _, err := varResolver.ResolveVariables(cfg.Key, varResolver.SimpleVariableReference, resolver.VarResolverFunc, true)
	if err != nil {
		return cfg, err
	}

	b1, err := wfc.ProcessTemplate(s)
	if err != nil {
		return cfg, err
	}

	cfg.Key = string(b1)
	return cfg, err
}

func (a *DatabricksActivity) resolveResponseFromCache(wfc *wfcase.WfCase, cacheConfig config.CacheConfig) (*har.Response, error) {
	cacheHarEntry, err := cacheoperation.Get(
		cacheConfig.LinkedServiceRef,
		a.Name()+";cache=true",
		cacheConfig.Key,
		"application/json",
		cachelks.WithNamespace(cacheConfig.Namespace), cachelks.WithHarPath(fmt.Sprintf("/%s/%s/%s;cache=true", string(config.MongoActivityType), string(a.definition.Statement.OpType), a.Name())))
	if err != nil {
		return nil, err
	}

	// the id takes the activity name in case ok because no other entry will be present. In case of cache miss ad additional entry will be there
	// together with the un-cached invokation
	entryId := a.Name()
	if cacheHarEntry.Response.Status != http.StatusOK {
		entryId = a.Name() + ";cache=true"
	}

	_ = wfc.SetHarEntry(entryId, cacheHarEntry)
	return cacheHarEntry.Response, nil
}

func (a *DatabricksActivity) saveResponseToCache(cacheConfig config.CacheConfig, data []byte) error {
	err := cacheoperation.Set(cacheConfig.LinkedServiceRef, cacheConfig.Key, data, cachelks.WithNamespace(cacheConfig.Namespace), cachelks.WithTTTL(cacheConfig.Ttl))
	if err != nil {
		return err
	}

	return nil
}
