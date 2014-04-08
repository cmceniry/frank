package main

import (
	"fmt"
	"github.com/cmceniry/golokia"
)

func main() {
	domains, _ := golokia.ListDomains("http://127.0.0.1:7025")
  for _, domain := range domains {
		beans, _ := golokia.ListBeans("http://127.0.0.1:7025", domain)
		for _, bean := range beans {
			props, _ := golokia.ListProperties("http://127.0.0.1:7025", domain, bean)
			for _, prop := range props {
				if val, err := golokia.GetAttr("http://127.0.0.1:7025", domain, bean, prop); err == nil {
					switch val.(type) {
					case string, float64, bool, nil:
						fmt.Printf("%s %s %s : %#v\n", domain, bean, prop, val)
					case map[string]interface{}:
						fmt.Printf("%s %s %s : %#v\n", domain, bean, prop, val)
					case []interface{}:
						fmt.Printf("%s %s %s : %#v\n", domain, bean, prop, val)
					default:
						fmt.Printf("UNK(%T) %s %s %s : %#v\n", val, domain, bean, prop, val)
					}
				} else {
					fmt.Printf("%s %s %s : %v\n", domain, bean, prop, err)
				}
			}
		}
	}
}
