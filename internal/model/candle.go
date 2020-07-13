/*
Copyright © 2020 A. Jensen <jensen.aaro@gmail.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package model

import (
	"time"
)

type Candle struct {
	Symbol    string    `yaml:"symbol,omitempty" json:"symbol,omitempty"`
	Timestamp time.Time `yaml:"timestamp,omitempty" json:"timestamp,omitempty"`
	Open      *float32  `yaml:"open,omitempty" json:"open,omitempty"`
	High      *float32  `yaml:"high,omitempty" json:"high,omitempty"`
	Low       *float32  `yaml:"low,omitempty" json:"low,omitempty"`
	Close     *float32  `yaml:"close,omitempty" json:"close,omitempty"`
	Volume    *float32  `yaml:"volume,omitempty" json:"volume,omitempty"`
}
