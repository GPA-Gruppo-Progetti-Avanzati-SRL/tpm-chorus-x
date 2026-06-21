package databricksactivity

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-chorus/orchestration/config"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/fileutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-databricks-common/sql"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type Statement struct {
	OpType   sql.DatabricksSQLOperationType `yaml:"op-type,omitempty" json:"op-type,omitempty" mapstructure:"op-type,omitempty"`
	Text     string                         `yaml:"text,omitempty" json:"text,omitempty" mapstructure:"text,omitempty"`
	MustFind bool                           `yaml:"must-find,omitempty" json:"must-find,omitempty" mapstructure:"must-find,omitempty"`
}

func (s Statement) IsZero() bool {
	return s.Text == ""
}

type Definition struct {
	LksName           string                                   `yaml:"lks-name,omitempty" json:"lks-name,omitempty" mapstructure:"lks-name,omitempty"`
	Statement         Statement                                `yaml:"statement,omitempty" json:"statement,omitempty" mapstructure:"statement,omitempty"`
	OnResponseActions config.OnResponseActions                 `yaml:"on-response,omitempty" json:"on-response,omitempty" mapstructure:"on-response,omitempty"`
	CacheConfig       config.CacheConfig                       `yaml:"with-cache,omitempty" json:"with-cache,omitempty" mapstructure:"with-cache,omitempty"`
	PII               config.PersonallyIdentifiableInformation `yaml:"pii,omitempty" mapstructure:"pii,omitempty" json:"pii,omitempty"`
}

func (d *Definition) IsZero() bool {
	return d.LksName == "" && d.Statement.IsZero()
}

func (d *Definition) WriteToFile(folderName string, fileName string, writeOpts ...fileutil.WriteOption) error {
	const semLogContext = SemLogPackageContext + "::write-to-file"
	fn := filepath.Join(folderName, fileName)
	log.Info().Str("file-name", fn).Msg(semLogContext)
	b, err := yaml.Marshal(d)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	err = fileutil.WriteFile(fn, b, os.ModePerm, writeOpts...)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return nil
}

func UnmarshalDefinition(def string, refs config.DataReferences) (Definition, error) {
	const semLogContext = SemLogPackageContext + "::unmarshal"

	var err error
	maDef := Definition{}

	if def != "" {
		data, ok := refs.Find(def)
		if len(data) == 0 || !ok {
			err = errors.New("cannot find activity definition")
			log.Error().Err(err).Str("def", def).Msg(semLogContext)
			return maDef, err
		}

		err = yaml.Unmarshal(data, &maDef)
		if err != nil {
			return maDef, err
		}
	}

	return maDef, nil
}
