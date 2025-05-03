package com.redis.riot;

import com.github.freva.asciitable.AsciiTable;

public enum AsciiTableBorder {

	NONE(AsciiTable.NO_BORDERS), BASIC(AsciiTable.BASIC_ASCII), OUTSIDE(AsciiTable.BASIC_ASCII_NO_DATA_SEPARATORS),
	TOP(AsciiTable.BASIC_ASCII_NO_DATA_SEPARATORS_NO_OUTSIDE_BORDER), DATA(AsciiTable.BASIC_ASCII_NO_OUTSIDE_BORDER),
	FANCY(AsciiTable.FANCY_ASCII);

	private final Character[] border;

	AsciiTableBorder(Character[] border) {
		this.border = border;
	}

	public Character[] getBorder() {
		return border;
	}

}
