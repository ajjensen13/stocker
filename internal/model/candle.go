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
	"github.com/jackc/pgtype"
)

type Candle struct {
	Symbol    pgtype.Text
	Timestamp pgtype.Timestamptz
	Open      pgtype.Float4
	High      pgtype.Float4
	Low       pgtype.Float4
	Close     pgtype.Float4
	Volume    pgtype.Float4
}
