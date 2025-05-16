Mô tả bài toán:

Phân tích/ tìm kiếm insights trong tập dữ liệu 30 ngày log contents giúp business có góc nhìn toàn diện về user story.

\=> những insights cần extract được:

Khách hàng thích xem thể loại nào nhất/xem nhiều nhất - Most Watch

Khẩu vị của khách hàng là những thể loại nào - Customer Taste

Tần suất hoạt động của khách hàng như thế nào - Activeness

Hình ảnh về nguồn dữ liệu ban đầu:

![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/img1.jpg)
Logic ETL Flow :
i2

Link Github source code:

Các bước em đã thực hiện:

**Đọc từng file data source, select ra các trường các cột cần thiết: Contract,AppName,TotalDuration**

**Thêm trường Date sau khi đọc mỗi file và union lại thành 1 dataframe chứa dữ liệu của cả 30 ngày**

**Script:**
i3
Kết quả khi đọc xog từng file và show ra console

i4
**Tiếp theo em tiến hành phân loại các AppName thành các nhóm App, ví dụ: với AppName = KPLUS thì sẽ được phân loại là Truyền Hình**

**Script:**

i5

Output thu được:

i6
**Tiếp theo tính tần suất hoạt động của khách hàng, với khách hàng có tần suất lớn hơn 20 ngày hoạt động thì sẽ được phân loại là High, với khách hàng có tần suất nhỏ hơn 20 và lớn hơn 10 sẽ được phân loại là Medium, còn lại sẽ là Low.**

Script:

i7
Output:

i8
**Bước tiếp, sử dụng hàm pivot để xổ ngang dữ liệu ra để có cái nhìn toàn cảnh về từng user sử dụng nhóm App nào và có tổng thời lượng sử dụng là bao nhiêu**

**Output:**

i9
**Dựa vào tổng thời lượng sử dụng cho từng nhóm App sẽ xác định được khách hàng xem nhóm nào nhiều nhất(Most Watch)**

i10
Output thu được:

i11
**Dựa vào tổng thời lượng xem cho mỗi nhóm App ta cũng có thể biết được khẩu vị xem của khách hàng là gì, khách hàng thích xem những nhóm App nào nhất, nhóm nào không xem(Customer Taste)**

**Script:**

i12
**Output:**

i13
Cuối cùng tiến hành save kết quả vào trong database

Kết quả ghi log full luồng ETL:

i14
Kết quả thực tế sau khi chạy full luồng ETL:

i15
NOTE:

+) Cần thêm filter để lọc các bản ghi có giá trị trường Contract là ip address
