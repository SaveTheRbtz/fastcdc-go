package main

import (
	"fmt"
	"html"
	"os"
)

// writeDistributionSVG emits stable, one-element-per-line SVG. Keeping the
// plot generator here avoids a plotting dependency and makes plot changes
// useful in ordinary text diffs.
func writeDistributionSVG(path, title string, bins []distributionBin) error {
	if len(bins) == 0 {
		return fmt.Errorf("write SVG: empty distribution")
	}
	const (
		width      = 960.0
		height     = 520.0
		left       = 72.0
		right      = 24.0
		top        = 56.0
		bottom     = 64.0
		plotWidth  = width - left - right
		plotHeight = height - top - bottom
	)
	maximum := 0.0
	for _, bin := range bins {
		maximum = maxFloat(maximum, bin.observed)
		maximum = maxFloat(maximum, bin.analytical)
	}
	if maximum == 0 {
		maximum = 1
	}

	svg := make([]byte, 0, 16<<10)
	svg = append(svg, `<?xml version="1.0" encoding="UTF-8"?>`...)
	svg = append(svg, '\n')
	svg = fmt.Appendf(svg, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f">`+"\n", width, height, width, height)
	svg = append(svg, `<rect width="100%" height="100%" fill="white"/>`...)
	svg = append(svg, '\n')
	svg = fmt.Appendf(svg, `<text x="%.1f" y="28" font-family="sans-serif" font-size="18">%s</text>`+"\n", left, html.EscapeString(title))
	for tick := 0; tick <= 4; tick++ {
		y := top + plotHeight*(1-float64(tick)/4)
		value := maximum * float64(tick) / 4
		svg = fmt.Appendf(svg, `<line x1="%.1f" y1="%.2f" x2="%.1f" y2="%.2f" stroke="#dddddd"/>`+"\n", left, y, left+plotWidth, y)
		svg = fmt.Appendf(svg, `<text x="%.1f" y="%.2f" text-anchor="end" font-family="monospace" font-size="11">%.4g</text>`+"\n", left-8, y+4, value)
	}
	barWidth := plotWidth / float64(len(bins))
	for i, bin := range bins {
		x := left + float64(i)*barWidth
		barHeight := plotHeight * bin.observed / maximum
		svg = fmt.Appendf(svg, `<rect x="%.3f" y="%.3f" width="%.3f" height="%.3f" fill="#4c78a8" fill-opacity="0.72"/>`+"\n",
			x, top+plotHeight-barHeight, maxFloat(barWidth, 0.2), barHeight)
	}
	svg = append(svg, `<polyline fill="none" stroke="#e45756" stroke-width="2" points="`...)
	for i, bin := range bins {
		x := left + (float64(i)+0.5)*barWidth
		y := top + plotHeight*(1-bin.analytical/maximum)
		svg = fmt.Appendf(svg, "%.3f,%.3f ", x, y)
	}
	svg = append(svg, `"/>`...)
	svg = append(svg, '\n')
	svg = fmt.Appendf(svg, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="black"/>`+"\n", left, top+plotHeight, left+plotWidth, top+plotHeight)
	svg = fmt.Appendf(svg, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="black"/>`+"\n", left, top, left, top+plotHeight)
	for tick := 0; tick <= 4; tick++ {
		x := left + plotWidth*float64(tick)/4
		index := tick * (len(bins) - 1) / 4
		svg = fmt.Appendf(svg, `<text x="%.2f" y="%.1f" text-anchor="middle" font-family="monospace" font-size="11">%s</text>`+"\n",
			x, top+plotHeight+22, formatBytes(int64(bins[index].lower)))
	}
	svg = fmt.Appendf(svg, `<text x="%.1f" y="%.1f" font-family="sans-serif" font-size="12" fill="#4c78a8">observed bins</text>`+"\n", left, height-16)
	svg = fmt.Appendf(svg, `<text x="%.1f" y="%.1f" font-family="sans-serif" font-size="12" fill="#e45756">independent-uniform model</text>`+"\n", left+130, height-16)
	svg = append(svg, "</svg>\n"...)
	if err := os.WriteFile(path, svg, 0o644); err != nil {
		return fmt.Errorf("write SVG: %w", err)
	}
	return nil
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
