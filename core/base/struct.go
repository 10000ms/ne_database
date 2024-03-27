package base

type WherePartItem struct {
	TargetColumn string         `json:"target_column"`
	Operate      DataComparator `json:"operate"`
	Args         [][]byte       `json:"args"`
}
