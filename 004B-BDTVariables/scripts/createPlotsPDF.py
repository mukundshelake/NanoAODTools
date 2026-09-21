#!/usr/bin/env python3
"""
Create a comprehensive PDF report with config details, config hash, and all plots.

Usage:
    python createPlotsPDF.py --configFile CONFIG.yaml --hash HASH --tag TAG --outputDir OUTPUT_DIR

The script will:
1. Create a title page with config hash, timestamp, and basic info
2. Add the config file content as a readable page
3. Embed all plots from the output directory
"""

import argparse
from pathlib import Path
from datetime import datetime
import yaml
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Image, Table, TableStyle, KeepTogether
from reportlab.lib import colors
from PIL import Image as PILImage
import io


def get_image_size(image_path, max_width=7*inch, max_height=9*inch):
    """Get scaled image dimensions to fit on page while maintaining aspect ratio."""
    try:
        img = PILImage.open(image_path)
        img_width, img_height = img.size
        aspect_ratio = img_height / img_width
        
        # Scale to fit within max dimensions
        if img_width > max_width:
            img_width = max_width
            img_height = img_width * aspect_ratio
        
        if img_height > max_height:
            img_height = max_height
            img_width = img_height / aspect_ratio
            
        return img_width, img_height
    except Exception as e:
        print(f"Warning: Could not determine image size for {image_path}: {e}")
        return 6*inch, 4*inch


def create_plots_pdf(config_file, config_hash, tag, output_dir):
    """Create a comprehensive PDF report with all plots and config details."""
    
    output_dir = Path(output_dir)
    plots_dir = output_dir / 'plots'
    
    # PDF output file
    pdf_file = output_dir / f'{tag}_{config_hash}_report.pdf'
    
    # Create PDF document
    doc = SimpleDocTemplate(
        str(pdf_file),
        pagesize=letter,
        rightMargin=0.5*inch,
        leftMargin=0.5*inch,
        topMargin=0.5*inch,
        bottomMargin=0.5*inch
    )
    
    # Container for the 'Flowable' objects
    story = []
    
    # Styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=12,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#1f4788'),
        spaceAfter=10,
        spaceBefore=10,
        fontName='Helvetica-Bold'
    )
    config_style = ParagraphStyle(
        'ConfigText',
        parent=styles['Normal'],
        fontSize=9,
        fontName='Courier',
        spaceAfter=2,
        leftIndent=0.2*inch
    )
    
    # ===== TITLE PAGE =====
    story.append(Spacer(1, 1*inch))
    story.append(Paragraph("Analysis Report", title_style))
    story.append(Spacer(1, 0.3*inch))
    
    # Config info table
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    info_data = [
        ['Config Hash:', config_hash],
        ['Tag:', tag],
        ['Generated:', timestamp],
    ]
    
    info_table = Table(info_data, colWidths=[2*inch, 4*inch])
    info_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e8eef7')),
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 11),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 1, colors.grey),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 0.5*inch))
    
    # ===== CONFIG FILE PAGE =====
    story.append(PageBreak())
    story.append(Paragraph("Configuration File", heading_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Load and display config
    try:
        with open(config_file, 'r') as f:
            config_content = f.read()
        
        # Limit config display to first 100 lines to avoid too long pages
        config_lines = config_content.split('\n')[:100]
        config_text = '\n'.join(config_lines)
        
        if len(config_content.split('\n')) > 100:
            config_text += "\n... (truncated for PDF length)"
        
        story.append(Paragraph(
            f"<font face='Courier' size='8'>{config_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')}</font>",
            config_style
        ))
    except Exception as e:
        story.append(Paragraph(f"Error reading config file: {e}", styles['Normal']))
    
    # ===== PLOTS PAGES =====
    story.append(PageBreak())
    story.append(Paragraph("Analysis Plots", heading_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Find all plot files recursively (handle nested folder structure like era/plotname.png)
    if plots_dir.exists():
        plot_files = sorted(list(plots_dir.glob('**/*.png')) + list(plots_dir.glob('**/*.jpg')))
        
        if plot_files:
            # Group plots by directory (era)
            plots_by_dir = {}
            for plot_file in plot_files:
                # Skip the report itself
                if 'report' in plot_file.name:
                    continue
                
                # Get the relative directory path from plots_dir
                rel_dir = plot_file.parent.relative_to(plots_dir)
                dir_name = str(rel_dir) if str(rel_dir) != '.' else 'Root'
                
                if dir_name not in plots_by_dir:
                    plots_by_dir[dir_name] = []
                plots_by_dir[dir_name].append(plot_file)
            
            # Add plots organized by directory/era
            for dir_name in sorted(plots_by_dir.keys()):
                story.append(Paragraph(f"Era: {dir_name}", styles['Heading2']))
                story.append(Spacer(1, 0.2*inch))
                
                for plot_file in sorted(plots_by_dir[dir_name]):
                    try:
                        # Add plot title (filename)
                        plot_title = plot_file.stem.replace('_', ' ').title()
                        story.append(Paragraph(plot_title, styles['Heading3']))
                        story.append(Spacer(1, 0.1*inch))
                        
                        # Add the image
                        if plot_file.suffix.lower() in ['.png', '.jpg']:
                            img_width, img_height = get_image_size(str(plot_file))
                            img = Image(str(plot_file), width=img_width, height=img_height)
                            story.append(img)
                        
                        story.append(Spacer(1, 0.2*inch))
                        
                        # Add page break after each plot to avoid crowding
                        story.append(PageBreak())
                        
                    except Exception as e:
                        print(f"Warning: Could not add plot {plot_file}: {e}")
                        story.append(Paragraph(f"Error adding plot {plot_file.name}: {e}", styles['Normal']))
                        story.append(Spacer(1, 0.2*inch))
        else:
            story.append(Paragraph("No plot files found in the plots directory.", styles['Normal']))
    else:
        story.append(Paragraph(f"Plots directory not found: {plots_dir}", styles['Normal']))
    
    # ===== BUILD PDF =====
    try:
        doc.build(story)
        print(f"PDF report successfully created: {pdf_file}")
        return str(pdf_file)
    except Exception as e:
        print(f"Error creating PDF: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description='Create a comprehensive PDF report with plots and config')
    parser.add_argument('--configFile', type=str, required=True,
                       help='Path to the config YAML file')
    parser.add_argument('--hash', type=str, required=True,
                       help='Config hash')
    parser.add_argument('--tag', type=str, required=True,
                       help='Tag name for the run')
    parser.add_argument('--outputDir', type=str, required=True,
                       help='Output directory containing the plots subdirectory')
    
    args = parser.parse_args()
    
    create_plots_pdf(args.configFile, args.hash, args.tag, args.outputDir)


if __name__ == '__main__':
    main()
