Mô tả bài toán:

Phân tích/ tìm kiếm insights trong tập dữ liệu 30 ngày log contents giúp business có góc nhìn toàn diện về user story.

\=> những insights cần extract được:

Khách hàng thích xem thể loại nào nhất/xem nhiều nhất - Most Watch

Khẩu vị của khách hàng là những thể loại nào - Customer Taste

Tần suất hoạt động của khách hàng như thế nào - Activeness

Hình ảnh về nguồn dữ liệu ban đầu:

![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/img1.jpg)<br>
Logic ETL Flow :<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/img2.jpg)<br>

Các bước em đã thực hiện:

**Đọc từng file data source, select ra các trường các cột cần thiết: Contract,AppName,TotalDuration**

**Thêm trường Date sau khi đọc mỗi file và union lại thành 1 dataframe chứa dữ liệu của cả 30 ngày**

**Script:**<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/img3.jpg)<br>

Kết quả khi đọc xog từng file và show ra console<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/img4.jpg)<br>

**Tiếp theo em tiến hành phân loại các AppName thành các nhóm App, ví dụ: với AppName = KPLUS thì sẽ được phân loại là Truyền Hình**

**Script:**<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/img5.jpg)<br>


Output thu được:<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/img6.jpg)<br>

**Tiếp theo tính tần suất hoạt động của khách hàng, với khách hàng có tần suất lớn hơn 20 ngày hoạt động thì sẽ được phân loại là High, với khách hàng có tần suất nhỏ hơn 20 và lớn hơn 10 sẽ được phân loại là Medium, còn lại sẽ là Low.**

Script:<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/Picture7.jpg)<br>

Output:<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/Picture8.jpg)<br>

**Bước tiếp, sử dụng hàm pivot để xổ ngang dữ liệu ra để có cái nhìn toàn cảnh về từng user sử dụng nhóm App nào và có tổng thời lượng sử dụng là bao nhiêu**
**Output:**<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/Picture9.jpg)<br>

**Dựa vào tổng thời lượng sử dụng cho từng nhóm App sẽ xác định được khách hàng xem nhóm nào nhiều nhất(Most Watch)**
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/Picture10.jpg)<br>
Output thu được:<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/Picture11.jpg)<br>

**Dựa vào tổng thời lượng xem cho mỗi nhóm App ta cũng có thể biết được khẩu vị xem của khách hàng là gì, khách hàng thích xem những nhóm App nào nhất, nhóm nào không xem(Customer Taste)**

**Script:**<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/Picture12.jpg)<br>
**Output:**<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/Picture13.jpg)<br>

Cuối cùng tiến hành save kết quả vào trong database<br>

Kết quả ghi log full luồng ETL:<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/Picture14.jpg)<br>

Kết quả thực tế sau khi chạy full luồng ETL:<br>
![Logo dự án](https://github.com/hkhanhdev/Customer360/blob/main/presentations/Picture15.jpg)<br>


NOTE:<br>
+) Cần thêm filter để lọc các bản ghi có giá trị trường Contract là ip address
