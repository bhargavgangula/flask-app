from flask import Flask, request, render_template, send_file, flash
import pandas as pd
import io
import os

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'

@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Excel Duplicate Remover</title>
        <style>
            body { font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto; padding: 20px; }
            .upload-form { background: #f5f5f5; padding: 20px; border-radius: 8px; }
            .file-input { margin: 10px 0; }
            .submit-btn { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
            .submit-btn:hover { background: #0056b3; }
        </style>
    </head>
    <body>
        <h1>📊 Excel Duplicate Remover</h1>
        <div class="upload-form">
            <form action="/upload" method="post" enctype="multipart/form-data">
                <div class="file-input">
                    <label><strong>First Excel File:</strong></label><br>
                    <input type="file" name="file1" accept=".xlsx,.xls" required>
                </div>
                <div class="file-input">
                    <label><strong>Second Excel File:</strong></label><br>
                    <input type="file" name="file2" accept=".xlsx,.xls" required>
                </div>
                <button type="submit" class="submit-btn">🚀 Process Files</button>
            </form>
        </div>
    </body>
    </html>
    '''

@app.route('/upload', methods=['POST'])
def upload_files():
    try:
        file1 = request.files['file1']
        file2 = request.files['file2']
        
        # Process files
        df1 = pd.read_excel(file1)
        df2 = pd.read_excel(file2)
        
        combined = pd.concat([df1, df2], ignore_index=True)
        unique_data = combined.drop_duplicates(subset=['Place ID'], keep='first')
        
        # Save to memory
        output = io.BytesIO()
        unique_data.to_excel(output, index=False)
        output.seek(0)
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='unique_places_data.xlsx'
        )
        
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == '__main__':
    app.run(debug=True)
