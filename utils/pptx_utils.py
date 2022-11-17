from pptx.dml.color import RGBColor


def replace_text_only(paragraph, target_text, color):
    # cur_text = paragrah.runs[0].text
    # new_text = cur_text.replace(str(search_str), str(repl_str))
    for idx, run in enumerate(paragraph.runs):
        if idx > 0:
            p = paragraph._p
            p.remove(run._r)
    paragraph.runs[0].font.color.rgb = RGBColor(*color)
    paragraph.runs[0].text = target_text


def replace_picture(slide, shape, image_path):
    with open(image_path, "rb") as file_obj:
        r_img_blob = file_obj.read()
    img_pic = shape._pic
    img_rid = img_pic.xpath("./p:blipFill/a:blip/@r:embed")[0]
    img_part = slide.part.related_part(img_rid)
    img_part._blob = r_img_blob


def replace_texts_with_diff_color(paragraph, target_texts, colors):
    for idx, run in enumerate(paragraph.runs):
        if idx > 1:
            p = paragraph._p
            p.remove(run._r)
    for run, target_text, color in zip(paragraph.runs, target_texts, colors):
        run.font.color.rgb = RGBColor(*color)
        run.text = target_text
