package hipstmr


type Transaction struct {
	Params *Params `json:"params"`
	Id string `json:"id"`
	Status string `json:"status"`
}
